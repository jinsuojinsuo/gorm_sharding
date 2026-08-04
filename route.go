package gorm_sharding

import (
	"reflect"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// timeFromStatement 从当前 GORM Statement 中提取精确的分表时间。
func timeFromStatement(db *gorm.DB, key string) (time.Time, bool) {
	// Create/Save 场景优先从 struct 本身取分表字段，Where 条件作为查询和写操作路由来源。
	if t, ok := timeFromReflect(db.Statement.ReflectValue, db.Statement.Schema, key); ok {
		return t, true
	}
	if where, ok := db.Statement.Clauses["WHERE"].Expression.(clause.Where); ok {
		return timeFromExprs(where.Exprs, key)
	}
	return time.Time{}, false
}

// timeRangeFromStatement 从当前 GORM Statement 中提取分表时间范围。
func timeRangeFromStatement(db *gorm.DB, key string) (time.Time, time.Time, bool) {
	if where, ok := db.Statement.Clauses["WHERE"].Expression.(clause.Where); ok {
		return timeRangeFromExprs(where.Exprs, key)
	}
	return time.Time{}, time.Time{}, false
}

// timeFromReflect 从模型值里读取分表字段时间。
func timeFromReflect(v reflect.Value, s *schema.Schema, key string) (time.Time, bool) {
	if !v.IsValid() || s == nil {
		return time.Time{}, false
	}
	fieldValue := func(value reflect.Value) (time.Time, bool) {
		for value.Kind() == reflect.Ptr {
			if value.IsNil() {
				return time.Time{}, false
			}
			value = value.Elem()
		}
		if value.Kind() != reflect.Struct {
			return time.Time{}, false
		}
		fieldName := key
		if f := s.LookUpField(key); f != nil {
			fieldName = f.Name
		}
		f := value.FieldByName(fieldName)
		if !f.IsValid() {
			return time.Time{}, false
		}
		t, ok := f.Interface().(time.Time)
		return t, ok && !t.IsZero()
	}

	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		if v.Len() == 0 {
			return time.Time{}, false
		}
		return fieldValue(elemAt(v, 0))
	}
	return fieldValue(v)
}

// timeFromExprs 从 GORM where 表达式中提取精确时间条件。
func timeFromExprs(exprs []clause.Expression, key string) (time.Time, bool) {
	for _, expr := range exprs {
		switch e := expr.(type) {
		case clause.Expr:
			// 支持 Where("created_at = ?", t) 以及 Where("created_at = ? AND name = ?", t, name)。
			if t, ok := timeFromSQLExpr(e.SQL, e.Vars, key); ok {
				return t, true
			}
		case clause.Eq:
			if columnMatches(e.Column, key) {
				return asTime(e.Value)
			}
		case clause.AndConditions:
			if t, ok := timeFromExprs(e.Exprs, key); ok {
				return t, true
			}
		case clause.OrConditions:
			if t, ok := timeFromExprs(e.Exprs, key); ok {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

// timeFromSQLExpr 从字符串 where 条件里找出分表字段等值条件，并按 ? 位置匹配对应参数。
func timeFromSQLExpr(sql string, vars []interface{}, key string) (time.Time, bool) {
	key = normalizeColumnName(key)
	lowerSQL := strings.ToLower(sql)
	candidates := []string{strings.ToLower(key), "`" + strings.ToLower(key) + "`"}
	for _, candidate := range candidates {
		for offset := 0; offset < len(lowerSQL); {
			pos := strings.Index(lowerSQL[offset:], candidate)
			if pos < 0 {
				break
			}
			pos += offset
			after := strings.TrimSpace(sql[pos+len(candidate):])
			if strings.HasPrefix(after, "=") {
				after = strings.TrimSpace(after[1:])
				if strings.HasPrefix(after, "?") {
					argIndex := strings.Count(sql[:pos], "?")
					if argIndex < len(vars) {
						return asTime(vars[argIndex])
					}
				}
			}
			offset = pos + len(candidate)
		}
	}
	return time.Time{}, false
}

// timeRangeFromExprs 从 GORM where 表达式中提取时间范围条件。
func timeRangeFromExprs(exprs []clause.Expression, key string) (time.Time, time.Time, bool) {
	var start, end time.Time
	for _, expr := range exprs {
		switch e := expr.(type) {
		case clause.Expr:
			// 支持 Where("created_at BETWEEN ? AND ?", start, end) 路由到范围内分表。
			if strings.Contains(strings.ToLower(e.SQL), strings.ToLower(key)) && len(e.Vars) >= 2 {
				if s, ok := asTime(e.Vars[0]); ok {
					if t, ok := asTime(e.Vars[1]); ok {
						return s, t, true
					}
				}
			}
		case clause.Gte:
			if columnMatches(e.Column, key) {
				start, _ = asTime(e.Value)
			}
		case clause.Lte:
			if columnMatches(e.Column, key) {
				end, _ = asTime(e.Value)
			}
		case clause.AndConditions:
			if s, t, ok := timeRangeFromExprs(e.Exprs, key); ok {
				return s, t, true
			}
		}
	}
	if !start.IsZero() && !end.IsZero() {
		return start, end, true
	}
	return time.Time{}, time.Time{}, false
}

// columnMatches 判断 GORM 条件里的列名是否等于分表字段。
func columnMatches(column interface{}, key string) bool {
	switch c := column.(type) {
	case clause.Column:
		return normalizeColumnName(c.Name) == normalizeColumnName(key)
	case string:
		return normalizeColumnName(c) == normalizeColumnName(key)
	}
	return false
}

// asTime 判断输入值是否是非零 time.Time。
func asTime(v interface{}) (time.Time, bool) {
	t, ok := v.(time.Time)
	return t, ok && !t.IsZero()
}

// tablesForRange 根据时间范围倒推出最多 MaxScanTables 张真实分表。
func tablesForRange(cfg ShardingConfig, start, end time.Time) []string {
	if end.Before(start) {
		start, end = end, start
	}
	out := make([]string, 0)
	seen := make(map[string]struct{})
	cursor := end
	for i := 0; i < cfg.MaxScanTables; i++ {
		table := cfg.tableName(cursor)
		if _, ok := seen[table]; !ok {
			out = append(out, table)
			seen[table] = struct{}{}
		}
		if !cursor.After(start) {
			break
		}
		// 从结束时间往前推，能优先命中较新的分表，并受 MaxScanTables 保护。
		cursor = cfg.Strategy.Prev(cursor)
	}
	return out
}
