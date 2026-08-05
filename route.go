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

// tablesFromExprs 从 where 表达式中提取分表路由目标，优先精确时间，其次时间范围。
func tablesFromExprs(exprs []clause.Expression, cfg ShardingConfig, key string) ([]string, bool) {
	if values, ok := timeValuesFromExprs(exprs, key); ok {
		return tablesForTimes(cfg, values), true
	}
	if start, end, ok := timeRangeFromExprs(exprs, key); ok {
		return tablesForRange(cfg, start, end), true
	}
	return nil, false
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
			if values, ok := timeValuesFromSQLExpr(e.SQL, e.Vars, key); ok && len(values) > 0 {
				return values[0], true
			}
		case clause.Eq:
			if columnMatches(e.Column, key) {
				return asTime(e.Value)
			}
		case clause.AndConditions:
			if t, ok := timeFromExprs(e.Exprs, key); ok {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

// timeValuesFromExprs 从 GORM where 表达式中提取精确时间值，支持 Eq、IN 和 OR 等值条件。
func timeValuesFromExprs(exprs []clause.Expression, key string) ([]time.Time, bool) {
	out := make([]time.Time, 0)
	for _, expr := range exprs {
		switch e := expr.(type) {
		case clause.Expr:
			if values, ok := timeValuesFromSQLExpr(e.SQL, e.Vars, key); ok {
				out = append(out, values...)
			}
		case clause.Eq:
			if columnMatches(e.Column, key) {
				if values, ok := asTimes(e.Value); ok {
					out = append(out, values...)
				}
			}
		case clause.IN:
			if columnMatches(e.Column, key) {
				if values, ok := asTimes(e.Values); ok {
					out = append(out, values...)
				}
			}
		case clause.AndConditions:
			if values, ok := timeValuesFromExprs(e.Exprs, key); ok {
				out = append(out, values...)
			}
		case clause.OrConditions:
			values, ok := timeValuesFromExprs(e.Exprs, key)
			if !ok {
				return nil, false
			}
			out = append(out, values...)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// timeValuesFromSQLExpr 从字符串 where 条件里找出分表字段等值或 IN 条件，并按 ? 位置匹配参数。
func timeValuesFromSQLExpr(sql string, vars []interface{}, key string) ([]time.Time, bool) {
	key = normalizeColumnName(key)
	lowerSQL := strings.ToLower(sql)
	candidates := columnCandidates(key)
	out := make([]time.Time, 0)
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
						if t, ok := asTime(vars[argIndex]); ok {
							out = append(out, t)
						}
					}
				}
			}
			if strings.HasPrefix(strings.ToLower(after), "in") {
				after = strings.TrimSpace(after[len("in"):])
				argIndex := strings.Count(sql[:pos], "?")
				if strings.HasPrefix(after, "?") && argIndex < len(vars) {
					if values, ok := asTimes(vars[argIndex]); ok {
						out = append(out, values...)
					}
				}
				if strings.HasPrefix(after, "(") {
					end := strings.Index(after, ")")
					if end > 0 {
						count := strings.Count(after[:end], "?")
						for i := 0; i < count && argIndex+i < len(vars); i++ {
							if t, ok := asTime(vars[argIndex+i]); ok {
								out = append(out, t)
							}
						}
					}
				}
			}
			offset = pos + len(candidate)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// timeRangeFromExprs 从 GORM where 表达式中提取时间范围条件。
func timeRangeFromExprs(exprs []clause.Expression, key string) (time.Time, time.Time, bool) {
	var start, end time.Time
	for _, expr := range exprs {
		switch e := expr.(type) {
		case clause.Expr:
			// 支持 BETWEEN 和 >=/< 这类常见字符串范围条件。
			if s, t, ok := timeRangeFromSQLExpr(e.SQL, e.Vars, key); ok {
				return s, t, true
			}
		case clause.Gt:
			if columnMatches(e.Column, key) {
				start, _ = asTime(e.Value)
			}
		case clause.Gte:
			if columnMatches(e.Column, key) {
				start, _ = asTime(e.Value)
			}
		case clause.Lt:
			if columnMatches(e.Column, key) {
				end, _ = asTime(e.Value)
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

// timeRangeFromSQLExpr 从字符串 where 条件中提取 BETWEEN 或上下界时间条件。
func timeRangeFromSQLExpr(sql string, vars []interface{}, key string) (time.Time, time.Time, bool) {
	key = normalizeColumnName(key)
	lowerSQL := strings.ToLower(sql)
	var start, end time.Time
	for _, candidate := range columnCandidates(key) {
		for offset := 0; offset < len(lowerSQL); {
			pos := strings.Index(lowerSQL[offset:], candidate)
			if pos < 0 {
				break
			}
			pos += offset
			after := strings.TrimSpace(sql[pos+len(candidate):])
			lowerAfter := strings.ToLower(after)
			argIndex := strings.Count(sql[:pos], "?")
			if strings.HasPrefix(lowerAfter, "between") && argIndex+1 < len(vars) {
				if s, ok := asTime(vars[argIndex]); ok {
					if t, ok := asTime(vars[argIndex+1]); ok {
						return s, t, true
					}
				}
			}
			if strings.HasPrefix(after, ">=") || strings.HasPrefix(after, ">") {
				if argIndex < len(vars) {
					start, _ = asTime(vars[argIndex])
				}
			}
			if strings.HasPrefix(after, "<=") || strings.HasPrefix(after, "<") {
				if argIndex < len(vars) {
					end, _ = asTime(vars[argIndex])
				}
			}
			offset = pos + len(candidate)
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

// asTimes 从单个 time.Time 或 time.Time 切片中提取非零分表时间。
func asTimes(v interface{}) ([]time.Time, bool) {
	if t, ok := asTime(v); ok {
		return []time.Time{t}, true
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, false
	}
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]time.Time, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		if t, ok := asTime(rv.Index(i).Interface()); ok {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// tablesForTimes 根据一组精确时间计算分表列表，并去重保序。
func tablesForTimes(cfg ShardingConfig, values []time.Time) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{})
	for _, value := range values {
		table := cfg.tableName(value)
		if _, ok := seen[table]; ok {
			continue
		}
		seen[table] = struct{}{}
		out = append(out, table)
	}
	return out
}

// columnCandidates 返回字符串 where 中可能出现的分表字段写法。
func columnCandidates(key string) []string {
	key = strings.ToLower(normalizeColumnName(key))
	return []string{
		key,
		"`" + key + "`",
		"." + key,
		".`" + key + "`",
	}
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
