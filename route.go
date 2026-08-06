package gorm_sharding

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

var inArgumentPattern = regexp.MustCompile(`(?i)\bin\s+\?`)

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
	if bounds, ok := timeRangeBoundsFromExprs(exprs, key); ok {
		return tablesForRangeBounds(cfg, bounds), true
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

// timeValuesFromSQLExpr 使用 Vitess AST 找出分表字段的等值或 IN 条件。
func timeValuesFromSQLExpr(sql string, vars []interface{}, key string) ([]time.Time, bool) {
	expr, ok := parseConditionSQL(sql)
	if !ok {
		return nil, false
	}
	return timeValuesFromVitessExpr(expr, vars, key)
}

// timeRangeFromExprs 从 GORM where 表达式中提取时间范围条件。
func timeRangeFromExprs(exprs []clause.Expression, key string) (time.Time, time.Time, bool) {
	bounds, ok := timeRangeBoundsFromExprs(exprs, key)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	return bounds.start, bounds.end, true
}

type timeRangeBounds struct {
	start        time.Time
	end          time.Time
	endExclusive bool
}

// timeRangeBoundsFromExprs 提取时间范围及上界是否排除，供分表边界计算使用。
func timeRangeBoundsFromExprs(exprs []clause.Expression, key string) (timeRangeBounds, bool) {
	bounds, found := collectRangeBounds(exprs, key)
	if !found || bounds.start.IsZero() || bounds.end.IsZero() {
		return timeRangeBounds{}, false
	}
	return bounds, true
}

// collectRangeBounds 收集表达式列表中的范围片段，并按 AND 语义取交集。
// 返回值允许只包含上界或下界，供调用方继续与其他 Where 表达式合并。
func collectRangeBounds(exprs []clause.Expression, key string) (timeRangeBounds, bool) {
	var bounds timeRangeBounds
	found := false
	for _, expr := range exprs {
		parsed, ok := rangeBoundsFromExpression(expr, key)
		if !ok {
			continue
		}
		if !found {
			bounds = parsed
			found = true
			continue
		}
		bounds = intersectRangeBounds(bounds, parsed)
	}
	return bounds, found
}

// rangeBoundsFromExpression 读取单个 GORM 条件中的完整或部分范围边界。
func rangeBoundsFromExpression(expr clause.Expression, key string) (timeRangeBounds, bool) {
	switch e := expr.(type) {
	case clause.Expr:
		return timeRangeBoundsFromSQLExpr(e.SQL, e.Vars, key)
	case clause.Gt, clause.Gte:
		var column interface{}
		var value interface{}
		switch condition := e.(type) {
		case clause.Gt:
			column, value = condition.Column, condition.Value
		case clause.Gte:
			column, value = condition.Column, condition.Value
		}
		if columnMatches(column, key) {
			if start, ok := asTime(value); ok {
				return timeRangeBounds{start: start}, true
			}
		}
	case clause.Lt, clause.Lte:
		var column interface{}
		var value interface{}
		exclusive := false
		switch condition := e.(type) {
		case clause.Lt:
			column, value, exclusive = condition.Column, condition.Value, true
		case clause.Lte:
			column, value = condition.Column, condition.Value
		}
		if columnMatches(column, key) {
			if end, ok := asTime(value); ok {
				return timeRangeBounds{end: end, endExclusive: exclusive}, true
			}
		}
	case clause.AndConditions:
		return collectRangeBounds(e.Exprs, key)
	}
	return timeRangeBounds{}, false
}

// intersectRangeBounds 合并两个 AND 范围条件：下界取较晚值，上界取较早值。
func intersectRangeBounds(left, right timeRangeBounds) timeRangeBounds {
	if !right.start.IsZero() && (left.start.IsZero() || right.start.After(left.start)) {
		left.start = right.start
	}
	if !right.end.IsZero() {
		if left.end.IsZero() || right.end.Before(left.end) {
			left.end = right.end
			left.endExclusive = right.endExclusive
		} else if right.end.Equal(left.end) {
			left.endExclusive = left.endExclusive || right.endExclusive
		}
	}
	return left
}

// timeRangeFromSQLExpr 从字符串 where 条件中提取 BETWEEN 或上下界时间条件。
func timeRangeFromSQLExpr(sql string, vars []interface{}, key string) (time.Time, time.Time, bool) {
	bounds, ok := timeRangeBoundsFromSQLExpr(sql, vars, key)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	return bounds.start, bounds.end, true
}

// timeRangeBoundsFromSQLExpr 使用 Vitess AST 从字符串条件中提取时间范围。
func timeRangeBoundsFromSQLExpr(sql string, vars []interface{}, key string) (timeRangeBounds, bool) {
	expr, ok := parseConditionSQL(sql)
	if !ok {
		return timeRangeBounds{}, false
	}
	return timeRangeBoundsFromVitessExpr(expr, vars, key)
}

// parseConditionSQL 把 GORM 的字符串条件包装为 SELECT，再交给 Vitess 解析。
func parseConditionSQL(sql string) (sqlparser.Expr, bool) {
	// GORM 的 IN ? 会把时间切片作为一个参数传入，而 Vitess 要求列表参数带圆括号。
	// 字段识别仍完全依赖后续 Vitess AST，不使用字符串匹配分表字段。
	sql = inArgumentPattern.ReplaceAllString(sql, "IN (?)")
	stmt, err := sqlparser.NewTestParser().Parse("SELECT 1 WHERE " + sql)
	if err != nil {
		return nil, false
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return nil, false
	}
	return selectStmt.Where.Expr, true
}

// rawStatementTables 从 Raw SQL 的 WHERE AST 中计算精确目标分表。
// routed 为 false 表示条件不含可识别的分表字段，调用方可按默认最近表策略处理。
func rawStatementTables(stmt sqlparser.Statement, vars []interface{}, cfg ShardingConfig, key string) ([]string, bool) {
	var where *sqlparser.Where
	switch s := stmt.(type) {
	case *sqlparser.Select:
		where = s.Where
	case *sqlparser.Update:
		where = s.Where
	case *sqlparser.Delete:
		where = s.Where
	default:
		return nil, false
	}
	if where == nil {
		return nil, false
	}
	if values, ok := timeValuesFromVitessExpr(where.Expr, vars, key); ok {
		return tablesForTimes(cfg, values), true
	}
	if bounds, ok := timeRangeBoundsFromVitessExpr(where.Expr, vars, key); ok {
		if bounds.start.IsZero() || bounds.end.IsZero() {
			return nil, false
		}
		return tablesForRangeBounds(cfg, bounds), true
	}
	return nil, false
}

// timeValuesFromVitessExpr 提取 AND/OR 组合中的精确分表时间；OR 的每一支都必须可精确路由。
func timeValuesFromVitessExpr(expr sqlparser.Expr, vars []interface{}, key string) ([]time.Time, bool) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, leftOK := timeValuesFromVitessExpr(e.Left, vars, key)
		right, rightOK := timeValuesFromVitessExpr(e.Right, vars, key)
		if !leftOK && !rightOK {
			return nil, false
		}
		return append(left, right...), true
	case *sqlparser.OrExpr:
		left, leftOK := timeValuesFromVitessExpr(e.Left, vars, key)
		right, rightOK := timeValuesFromVitessExpr(e.Right, vars, key)
		if !leftOK || !rightOK {
			return nil, false
		}
		return append(left, right...), true
	case *sqlparser.ComparisonExpr:
		if !vitessColumnMatches(e.Left, key) {
			return nil, false
		}
		switch e.Operator {
		case sqlparser.EqualOp:
			return timeValuesFromVitessValue(e.Right, vars)
		case sqlparser.InOp:
			return timeValuesFromVitessValue(e.Right, vars)
		}
	}
	return nil, false
}

// timeRangeBoundsFromVitessExpr 仅接受 AND 组合的范围条件，避免 OR 条件被错误缩窄。
func timeRangeBoundsFromVitessExpr(expr sqlparser.Expr, vars []interface{}, key string) (timeRangeBounds, bool) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, leftOK := timeRangeBoundsFromVitessExpr(e.Left, vars, key)
		right, rightOK := timeRangeBoundsFromVitessExpr(e.Right, vars, key)
		if !leftOK && !rightOK {
			return timeRangeBounds{}, false
		}
		if !leftOK {
			return right, true
		}
		if !rightOK {
			return left, true
		}
		// AND 条件的下界取较晚值、上界取较早值，不能依赖条件书写顺序。
		if !right.start.IsZero() && (left.start.IsZero() || right.start.After(left.start)) {
			left.start = right.start
		}
		if !right.end.IsZero() {
			if left.end.IsZero() || right.end.Before(left.end) {
				left.end = right.end
				left.endExclusive = right.endExclusive
			} else if right.end.Equal(left.end) {
				// 相同上界时，只要任一条件为 <，合并后也必须保持开区间。
				left.endExclusive = left.endExclusive || right.endExclusive
			}
		}
		return left, true
	case *sqlparser.BetweenExpr:
		if !e.IsBetween || !vitessColumnMatches(e.Left, key) {
			return timeRangeBounds{}, false
		}
		start, startOK := timeFromVitessValue(e.From, vars)
		end, endOK := timeFromVitessValue(e.To, vars)
		if !startOK || !endOK {
			return timeRangeBounds{}, false
		}
		return timeRangeBounds{start: start, end: end}, true
	case *sqlparser.ComparisonExpr:
		if !vitessColumnMatches(e.Left, key) {
			return timeRangeBounds{}, false
		}
		value, ok := timeFromVitessValue(e.Right, vars)
		if !ok {
			return timeRangeBounds{}, false
		}
		switch e.Operator {
		case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp:
			return timeRangeBounds{start: value}, true
		case sqlparser.LessThanOp:
			return timeRangeBounds{end: value, endExclusive: true}, true
		case sqlparser.LessEqualOp:
			return timeRangeBounds{end: value}, true
		}
	}
	return timeRangeBounds{}, false
}

// vitessColumnMatches 判断 Vitess AST 中的列是否等于分表字段。
func vitessColumnMatches(expr sqlparser.Expr, key string) bool {
	column, ok := expr.(*sqlparser.ColName)
	return ok && normalizeColumnName(column.Name.String()) == normalizeColumnName(key)
}

// timeValuesFromVitessValue 从 AST 值表达式中读取一个或多个绑定时间参数。
func timeValuesFromVitessValue(expr sqlparser.Expr, vars []interface{}) ([]time.Time, bool) {
	if value, ok := timeFromVitessValue(expr, vars); ok {
		return []time.Time{value}, true
	}
	if argument, ok := expr.(*sqlparser.Argument); ok {
		if index, ok := vitessArgumentIndex(argument, len(vars)); ok {
			return asTimes(vars[index])
		}
	}
	tuple, ok := expr.(sqlparser.ValTuple)
	if !ok {
		return nil, false
	}
	out := make([]time.Time, 0, len(tuple))
	for _, item := range tuple {
		values, ok := timeValuesFromVitessValue(item, vars)
		if !ok {
			return nil, false
		}
		out = append(out, values...)
	}
	return out, len(out) > 0
}

// timeFromVitessValue 根据 Vitess 自动生成的 v1、v2 占位符读取 GORM 参数。
func timeFromVitessValue(expr sqlparser.Expr, vars []interface{}) (time.Time, bool) {
	argument, ok := expr.(*sqlparser.Argument)
	if !ok {
		return time.Time{}, false
	}
	index, ok := vitessArgumentIndex(argument, len(vars))
	if !ok {
		return time.Time{}, false
	}
	return asTime(vars[index])
}

// vitessArgumentIndex 把 Vitess 自动生成的 v1、v2 占位符转换为 GORM 参数下标。
func vitessArgumentIndex(argument *sqlparser.Argument, length int) (int, bool) {
	if !strings.HasPrefix(argument.Name, "v") {
		return 0, false
	}
	index, err := strconv.Atoi(strings.TrimPrefix(argument.Name, "v"))
	if err != nil || index <= 0 || index > length {
		return 0, false
	}
	return index - 1, true
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

// tablesForRangeBounds 根据时间范围倒推出最多 MaxScanTables 张真实分表。
func tablesForRangeBounds(cfg ShardingConfig, bounds timeRangeBounds) []string {
	start, end := bounds.start, bounds.end
	if end.Before(start) {
		// 原始 WHERE 条件必定为空集，不能交换边界后扫描无关的历史分表。
		return nil
	}
	if bounds.endExclusive && cfg.tableName(end) != cfg.tableName(end.Add(-time.Nanosecond)) {
		// 上界恰好位于下一个分片起点时，该分片不属于 [start, end)。
		end = end.Add(-time.Nanosecond)
	}
	out := make([]string, 0)
	seen := make(map[string]struct{})
	cursor := end
	startTable := cfg.tableName(start)
	for i := 0; i < cfg.MaxScanTables; i++ {
		table := cfg.tableName(cursor)
		if _, ok := seen[table]; !ok {
			out = append(out, table)
			seen[table] = struct{}{}
		}
		// 已经包含起始分片后立即停止；比较具体时间会在半开区间中多退一张表。
		if table == startTable {
			break
		}
		// 从结束时间往前推，能优先命中较新的分表，并受 MaxScanTables 保护。
		cursor = cfg.Strategy.Prev(cursor)
	}
	return out
}
