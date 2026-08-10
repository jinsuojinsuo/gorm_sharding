package gorm_sharding

import (
	"fmt"
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
func tablesFromExprs(exprs []clause.Expression, cfg ShardingConfig, key string) ([]string, bool, error) {
	if err := normalizeShardingExprs(exprs, cfg, key); err != nil {
		return nil, false, err
	}
	likeBounds, hasLike, err := likeRangeBoundsFromExprs(exprs, cfg, key)
	if err != nil {
		return nil, false, err
	}
	values, hasValues, err := timeValuesFromExprs(exprs, cfg, key)
	if err != nil {
		return nil, false, err
	}
	rangeBounds, hasRange, err := timeRangeBoundsFromExprs(exprs, cfg, key)
	if err != nil {
		return nil, false, err
	}
	if tables, ok := tablesForShardingConditions(cfg, values, hasValues, rangeBounds, hasRange, likeBounds, hasLike); ok {
		return tables, true, nil
	}
	// 所有分表字段条件已在预处理阶段完成校验。此处无法得出精确有限范围时，
	// 交给调用方扫描最近周期；该降级行为由 MaxScanTables 限制并在文档中明确说明。
	return nil, false, nil
}

// tablesForShardingConditions 按 AND 语义合并精确时间、显式范围和 LIKE 前缀范围。
func tablesForShardingConditions(cfg ShardingConfig, values []time.Time, hasValues bool, rangeBounds timeRangeBounds, hasRange bool, likeBounds timeRangeBounds, hasLike bool) ([]string, bool) {
	var bounds timeRangeBounds
	hasBounds := false
	if hasRange {
		bounds = rangeBounds
		hasBounds = true
	}
	if hasLike {
		if hasBounds {
			bounds = intersectRangeBounds(bounds, likeBounds)
		} else {
			bounds = likeBounds
			hasBounds = true
		}
	}
	if hasValues {
		if hasBounds {
			values = filterTimesByBounds(values, bounds)
		}
		return tablesForTimes(cfg, values), true
	}
	if hasBounds {
		return tablesForRangeBounds(cfg, bounds), true
	}
	return nil, false
}

// filterTimesByBounds 保留满足完整范围条件的精确时间值。
func filterTimesByBounds(values []time.Time, bounds timeRangeBounds) []time.Time {
	out := make([]time.Time, 0, len(values))
	for _, value := range values {
		if !bounds.start.IsZero() && (value.Before(bounds.start) || (bounds.startExclusive && value.Equal(bounds.start))) {
			continue
		}
		if !bounds.end.IsZero() && (value.After(bounds.end) || (bounds.endExclusive && value.Equal(bounds.end))) {
			continue
		}
		out = append(out, value)
	}
	return out
}

// normalizeShardingExprs 在计算路由前遍历全部 WHERE 条件，归一化并校验所有分表字段参数。
// 路由选择可以只使用其中一部分条件，但不能因此遗漏后续条件的参数改写或错误。
func normalizeShardingExprs(exprs []clause.Expression, cfg ShardingConfig, key string) error {
	for index, expression := range exprs {
		switch expr := expression.(type) {
		case clause.Expr:
			parsed, ok := parseConditionSQL(expr.SQL)
			if !ok {
				if sqlMentionsColumn(expr.SQL, key) {
					return fmt.Errorf("gorm_sharding: unsupported sharding time expression")
				}
				continue
			}
			if err := normalizeVitessShardingExpr(parsed, expr.Vars, cfg, key); err != nil {
				return err
			}
		case clause.Eq:
			if !columnMatches(expr.Column, key) {
				continue
			}
			value, err := normalizeShardingClauseValue(expr.Value, cfg.Location)
			if err != nil {
				return err
			}
			expr.Value = value
			exprs[index] = expr
		case clause.IN:
			if !columnMatches(expr.Column, key) {
				continue
			}
			_, normalized, err := normalizeShardingClauseValues(expr.Values, cfg.Location)
			if err != nil {
				return err
			}
			values, ok := normalized.([]interface{})
			if !ok {
				return fmt.Errorf("gorm_sharding: unsupported sharding time expression")
			}
			expr.Values = values
			exprs[index] = expr
		case clause.Gt:
			if !columnMatches(expr.Column, key) {
				continue
			}
			value, err := normalizeShardingClauseValue(expr.Value, cfg.Location)
			if err != nil {
				return err
			}
			expr.Value = value
			exprs[index] = expr
		case clause.Gte:
			if !columnMatches(expr.Column, key) {
				continue
			}
			value, err := normalizeShardingClauseValue(expr.Value, cfg.Location)
			if err != nil {
				return err
			}
			expr.Value = value
			exprs[index] = expr
		case clause.Lt:
			if !columnMatches(expr.Column, key) {
				continue
			}
			value, err := normalizeShardingClauseValue(expr.Value, cfg.Location)
			if err != nil {
				return err
			}
			expr.Value = value
			exprs[index] = expr
		case clause.Lte:
			if !columnMatches(expr.Column, key) {
				continue
			}
			value, err := normalizeShardingClauseValue(expr.Value, cfg.Location)
			if err != nil {
				return err
			}
			expr.Value = value
			exprs[index] = expr
		case clause.AndConditions:
			if err := normalizeShardingExprs(expr.Exprs, cfg, key); err != nil {
				return err
			}
			exprs[index] = expr
		case clause.OrConditions:
			if err := normalizeShardingExprs(expr.Exprs, cfg, key); err != nil {
				return err
			}
			exprs[index] = expr
		case clause.NotConditions:
			if err := normalizeShardingExprs(expr.Exprs, cfg, key); err != nil {
				return err
			}
			exprs[index] = expr
		}
	}
	return nil
}

// normalizeVitessShardingExpr 归一化 Raw SQL 或 clause.Expr AST 中全部分表字段参数。
func normalizeVitessShardingExpr(expr sqlparser.Expr, vars []interface{}, cfg ShardingConfig, key string) error {
	switch node := expr.(type) {
	case *sqlparser.AndExpr:
		if err := normalizeVitessShardingExpr(node.Left, vars, cfg, key); err != nil {
			return err
		}
		return normalizeVitessShardingExpr(node.Right, vars, cfg, key)
	case *sqlparser.OrExpr:
		if err := normalizeVitessShardingExpr(node.Left, vars, cfg, key); err != nil {
			return err
		}
		return normalizeVitessShardingExpr(node.Right, vars, cfg, key)
	case *sqlparser.NotExpr:
		return normalizeVitessShardingExpr(node.Expr, vars, cfg, key)
	case *sqlparser.BetweenExpr:
		if !vitessColumnMatches(node.Left, key) {
			break
		}
		if _, err := timeFromVitessValue(node.From, vars, cfg.Location); err != nil {
			return err
		}
		_, err := timeFromVitessValue(node.To, vars, cfg.Location)
		return err
	case *sqlparser.ComparisonExpr:
		if !vitessColumnMatches(node.Left, key) {
			break
		}
		switch node.Operator {
		case sqlparser.EqualOp, sqlparser.InOp:
			_, _, err := timeValuesFromVitessValue(node.Right, vars, cfg.Location)
			return err
		case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp, sqlparser.LessThanOp, sqlparser.LessEqualOp:
			_, err := timeFromVitessValue(node.Right, vars, cfg.Location)
			return err
		case sqlparser.LikeOp:
			pattern, err := stringFromVitessValue(node.Right, vars)
			if err != nil {
				return err
			}
			_, _, err = parseShardingLikePrefix(pattern, cfg.Location)
			return err
		default:
			return fmt.Errorf("gorm_sharding: unsupported sharding time expression")
		}
	}
	if vitessExprReferencesColumn(expr, key) {
		return fmt.Errorf("gorm_sharding: unsupported sharding time expression")
	}
	return nil
}

// likeRangeBoundsFromExprs 识别 created_at LIKE ? 的连续日期前缀并转换为半开时间范围。
func likeRangeBoundsFromExprs(exprs []clause.Expression, cfg ShardingConfig, key string) (timeRangeBounds, bool, error) {
	var bounds timeRangeBounds
	found := false
	for _, expression := range exprs {
		expr, ok := expression.(clause.Expr)
		if !ok {
			continue
		}
		parsed, ok := parseConditionSQL(expr.SQL)
		if !ok {
			continue
		}
		parsedBounds, matched, err := likeRangeBoundsFromVitessExpr(parsed, expr.Vars, cfg, key)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		if !matched {
			continue
		}
		if found {
			bounds = intersectRangeBounds(bounds, parsedBounds)
		} else {
			bounds = parsedBounds
			found = true
		}
	}
	return bounds, found, nil
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
	cfg := ShardingConfig{Location: time.Local}
	for _, expr := range exprs {
		switch e := expr.(type) {
		case clause.Expr:
			// 支持 Where("created_at = ?", t) 以及 Where("created_at = ? AND name = ?", t, name)。
			if values, ok, err := timeValuesFromSQLExpr(e.SQL, e.Vars, cfg, key); err == nil && ok && len(values) > 0 {
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
func timeValuesFromExprs(exprs []clause.Expression, cfg ShardingConfig, key string) ([]time.Time, bool, error) {
	var out []time.Time
	found := false
	for index, expr := range exprs {
		var values []time.Time
		matched := false
		switch e := expr.(type) {
		case clause.Expr:
			parsedValues, ok, err := timeValuesFromSQLExpr(e.SQL, e.Vars, cfg, key)
			if err != nil {
				return nil, false, err
			}
			values, matched = parsedValues, ok
		case clause.Eq:
			if columnMatches(e.Column, key) {
				value, err := normalizeShardingClauseValue(e.Value, cfg.Location)
				if err != nil {
					return nil, false, err
				}
				e.Value = value
				exprs[index] = e
				values, matched = []time.Time{value}, true
			}
		case clause.IN:
			if columnMatches(e.Column, key) {
				parsedValues, normalized, err := normalizeShardingClauseValues(e.Values, cfg.Location)
				if err != nil {
					return nil, false, err
				}
				normalizedValues, ok := normalized.([]interface{})
				if !ok {
					return nil, false, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
				}
				e.Values = normalizedValues
				exprs[index] = e
				values, matched = parsedValues, true
			}
		case clause.AndConditions:
			parsedValues, ok, err := timeValuesFromExprs(e.Exprs, cfg, key)
			if err != nil {
				return nil, false, err
			}
			if ok {
				e.Exprs = e.Exprs
				exprs[index] = e
				values, matched = parsedValues, true
			}
		case clause.OrConditions:
			parsedValues, ok, err := timeValuesFromOrExprs(e.Exprs, cfg, key)
			if err != nil || !ok {
				return nil, false, err
			}
			e.Exprs = e.Exprs
			exprs[index] = e
			values, matched = parsedValues, true
		}
		if !matched {
			continue
		}
		if !found {
			out = values
			found = true
			continue
		}
		out = intersectTimeValues(out, values)
	}
	if !found {
		return nil, false, nil
	}
	return out, true, nil
}

// timeValuesFromOrExprs 仅当 OR 的每个分支都能确定为精确时间集合时才返回并集。
func timeValuesFromOrExprs(exprs []clause.Expression, cfg ShardingConfig, key string) ([]time.Time, bool, error) {
	out := make([]time.Time, 0)
	for _, expr := range exprs {
		values, ok, err := timeValuesFromExprs([]clause.Expression{expr}, cfg, key)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		for _, value := range values {
			if !containsTime(out, value) {
				out = append(out, value)
			}
		}
	}
	return out, true, nil
}

// timeValuesFromSQLExpr 使用 Vitess AST 找出分表字段的等值或 IN 条件。
func timeValuesFromSQLExpr(sql string, vars []interface{}, cfg ShardingConfig, key string) ([]time.Time, bool, error) {
	expr, ok := parseConditionSQL(sql)
	if !ok {
		if sqlMentionsColumn(sql, key) {
			return nil, false, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
		}
		return nil, false, nil
	}
	return timeValuesFromVitessExpr(expr, vars, cfg, key)
}

// timeRangeFromExprs 从 GORM where 表达式中提取时间范围条件。
func timeRangeFromExprs(exprs []clause.Expression, key string) (time.Time, time.Time, bool) {
	cfg := ShardingConfig{Location: time.Local}
	bounds, ok, err := timeRangeBoundsFromExprs(exprs, cfg, key)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	return bounds.start, bounds.end, true
}

type timeRangeBounds struct {
	start          time.Time
	startExclusive bool
	end            time.Time
	endExclusive   bool
}

// timeRangeBoundsFromExprs 提取时间范围及上下界是否排除，供分表边界计算使用。
func timeRangeBoundsFromExprs(exprs []clause.Expression, cfg ShardingConfig, key string) (timeRangeBounds, bool, error) {
	bounds, found, err := collectRangeBounds(exprs, cfg, key)
	if err != nil {
		return timeRangeBounds{}, false, err
	}
	if !found || bounds.start.IsZero() || bounds.end.IsZero() {
		return timeRangeBounds{}, false, nil
	}
	return bounds, true, nil
}

// collectRangeBounds 收集表达式列表中的范围片段，并按 AND 语义取交集。
// 返回值允许只包含上界或下界，供调用方继续与其他 Where 表达式合并。
func collectRangeBounds(exprs []clause.Expression, cfg ShardingConfig, key string) (timeRangeBounds, bool, error) {
	var bounds timeRangeBounds
	found := false
	for index, expr := range exprs {
		parsed, updated, ok, err := rangeBoundsFromExpression(expr, cfg, key)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		if !ok {
			continue
		}
		exprs[index] = updated
		if !found {
			bounds = parsed
			found = true
			continue
		}
		bounds = intersectRangeBounds(bounds, parsed)
	}
	return bounds, found, nil
}

// rangeBoundsFromExpression 读取单个 GORM 条件中的完整或部分范围边界。
func rangeBoundsFromExpression(expr clause.Expression, cfg ShardingConfig, key string) (timeRangeBounds, clause.Expression, bool, error) {
	switch e := expr.(type) {
	case clause.Expr:
		bounds, ok, err := timeRangeBoundsFromSQLExpr(e.SQL, e.Vars, cfg, key)
		return bounds, e, ok, err
	case clause.Gt, clause.Gte:
		var column interface{}
		var value interface{}
		exclusive := false
		switch condition := e.(type) {
		case clause.Gt:
			column, value, exclusive = condition.Column, condition.Value, true
		case clause.Gte:
			column, value = condition.Column, condition.Value
		}
		if columnMatches(column, key) {
			start, err := normalizeShardingClauseValue(value, cfg.Location)
			if err != nil {
				return timeRangeBounds{}, expr, false, err
			}
			switch condition := e.(type) {
			case clause.Gt:
				condition.Value = start
				return timeRangeBounds{start: start, startExclusive: exclusive}, condition, true, nil
			case clause.Gte:
				condition.Value = start
				return timeRangeBounds{start: start, startExclusive: exclusive}, condition, true, nil
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
			end, err := normalizeShardingClauseValue(value, cfg.Location)
			if err != nil {
				return timeRangeBounds{}, expr, false, err
			}
			switch condition := e.(type) {
			case clause.Lt:
				condition.Value = end
				return timeRangeBounds{end: end, endExclusive: exclusive}, condition, true, nil
			case clause.Lte:
				condition.Value = end
				return timeRangeBounds{end: end, endExclusive: exclusive}, condition, true, nil
			}
		}
	case clause.AndConditions:
		bounds, ok, err := collectRangeBounds(e.Exprs, cfg, key)
		return bounds, e, ok, err
	}
	return timeRangeBounds{}, expr, false, nil
}

// intersectRangeBounds 合并两个 AND 范围条件：下界取较晚值，上界取较早值，并保留开闭边界。
func intersectRangeBounds(left, right timeRangeBounds) timeRangeBounds {
	if !right.start.IsZero() && (left.start.IsZero() || right.start.After(left.start)) {
		left.start = right.start
		left.startExclusive = right.startExclusive
	} else if !right.start.IsZero() && right.start.Equal(left.start) {
		// 相同下界时，只要任一条件为 >，合并后也必须保持开区间。
		left.startExclusive = left.startExclusive || right.startExclusive
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
	cfg := ShardingConfig{Location: time.Local}
	bounds, ok, err := timeRangeBoundsFromSQLExpr(sql, vars, cfg, key)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	return bounds.start, bounds.end, true
}

// timeRangeBoundsFromSQLExpr 使用 Vitess AST 从字符串条件中提取时间范围。
func timeRangeBoundsFromSQLExpr(sql string, vars []interface{}, cfg ShardingConfig, key string) (timeRangeBounds, bool, error) {
	expr, ok := parseConditionSQL(sql)
	if !ok {
		if sqlMentionsColumn(sql, key) {
			return timeRangeBounds{}, false, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
		}
		return timeRangeBounds{}, false, nil
	}
	return timeRangeBoundsFromVitessExpr(expr, vars, cfg, key)
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
func rawStatementTables(stmt sqlparser.Statement, vars []interface{}, cfg ShardingConfig, key string) ([]string, bool, error) {
	var where *sqlparser.Where
	switch s := stmt.(type) {
	case *sqlparser.Select:
		where = s.Where
	case *sqlparser.Update:
		where = s.Where
	case *sqlparser.Delete:
		where = s.Where
	default:
		return nil, false, nil
	}
	if where == nil {
		return nil, false, nil
	}
	if err := normalizeVitessShardingExpr(where.Expr, vars, cfg, key); err != nil {
		return nil, false, err
	}
	likeBounds, hasLike, err := likeRangeBoundsFromVitessExpr(where.Expr, vars, cfg, key)
	if err != nil {
		return nil, false, err
	}
	values, hasValues, err := timeValuesFromVitessExpr(where.Expr, vars, cfg, key)
	if err != nil {
		return nil, false, err
	}
	rangeBounds, hasRange, err := timeRangeBoundsFromVitessExpr(where.Expr, vars, cfg, key)
	if err != nil {
		return nil, false, err
	}
	if hasRange && (rangeBounds.start.IsZero() || rangeBounds.end.IsZero()) {
		hasRange = false
	}
	if tables, ok := tablesForShardingConditions(cfg, values, hasValues, rangeBounds, hasRange, likeBounds, hasLike); ok {
		return tables, true, nil
	}
	// normalizeVitessShardingExpr 已拒绝非法或无法解释的分表字段表达式。
	// 合法但无法精确路由的条件由调用方退化为最近周期扫描。
	return nil, false, nil
}

// timeValuesFromVitessExpr 提取 AND/OR 组合中的精确分表时间。
// OR 任一分支无法化为精确时间时，调用方会按最近周期扫描；参数校验已由预处理完成。
func timeValuesFromVitessExpr(expr sqlparser.Expr, vars []interface{}, cfg ShardingConfig, key string) ([]time.Time, bool, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, leftOK, err := timeValuesFromVitessExpr(e.Left, vars, cfg, key)
		if err != nil {
			return nil, false, err
		}
		right, rightOK, err := timeValuesFromVitessExpr(e.Right, vars, cfg, key)
		if err != nil {
			return nil, false, err
		}
		if !leftOK {
			return right, rightOK, nil
		}
		if !rightOK {
			return left, true, nil
		}
		return intersectTimeValues(left, right), true, nil
	case *sqlparser.OrExpr:
		left, leftOK, err := timeValuesFromVitessExpr(e.Left, vars, cfg, key)
		if err != nil {
			return nil, false, err
		}
		right, rightOK, err := timeValuesFromVitessExpr(e.Right, vars, cfg, key)
		if err != nil {
			return nil, false, err
		}
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		return append(left, right...), true, nil
	case *sqlparser.ComparisonExpr:
		if !vitessColumnMatches(e.Left, key) {
			return nil, false, nil
		}
		switch e.Operator {
		case sqlparser.EqualOp:
			return timeValuesFromVitessValue(e.Right, vars, cfg.Location)
		case sqlparser.InOp:
			return timeValuesFromVitessValue(e.Right, vars, cfg.Location)
		}
	}
	return nil, false, nil
}

// intersectTimeValues 返回两个精确时间集合在同一 AND 条件中的交集。
func intersectTimeValues(left, right []time.Time) []time.Time {
	out := make([]time.Time, 0)
	for _, candidate := range left {
		if containsTime(out, candidate) {
			continue
		}
		for _, other := range right {
			if candidate.Equal(other) {
				out = append(out, candidate)
				break
			}
		}
	}
	return out
}

func containsTime(values []time.Time, target time.Time) bool {
	for _, value := range values {
		if value.Equal(target) {
			return true
		}
	}
	return false
}

// timeRangeBoundsFromVitessExpr 仅接受 AND 组合的范围条件，避免 OR 条件被错误缩窄。
func timeRangeBoundsFromVitessExpr(expr sqlparser.Expr, vars []interface{}, cfg ShardingConfig, key string) (timeRangeBounds, bool, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, leftOK, err := timeRangeBoundsFromVitessExpr(e.Left, vars, cfg, key)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		right, rightOK, err := timeRangeBoundsFromVitessExpr(e.Right, vars, cfg, key)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		if !leftOK && !rightOK {
			return timeRangeBounds{}, false, nil
		}
		if !leftOK {
			return right, true, nil
		}
		if !rightOK {
			return left, true, nil
		}
		// AND 条件的下界取较晚值、上界取较早值，不能依赖条件书写顺序。
		if !right.start.IsZero() && (left.start.IsZero() || right.start.After(left.start)) {
			left.start = right.start
			left.startExclusive = right.startExclusive
		} else if !right.start.IsZero() && right.start.Equal(left.start) {
			// 相同下界时，只要任一条件为 >，合并后也必须保持开区间。
			left.startExclusive = left.startExclusive || right.startExclusive
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
		return left, true, nil
	case *sqlparser.BetweenExpr:
		if !e.IsBetween || !vitessColumnMatches(e.Left, key) {
			return timeRangeBounds{}, false, nil
		}
		start, err := timeFromVitessValue(e.From, vars, cfg.Location)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		end, err := timeFromVitessValue(e.To, vars, cfg.Location)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		return timeRangeBounds{start: start, end: end}, true, nil
	case *sqlparser.ComparisonExpr:
		if !vitessColumnMatches(e.Left, key) {
			return timeRangeBounds{}, false, nil
		}
		switch e.Operator {
		case sqlparser.GreaterThanOp, sqlparser.GreaterEqualOp, sqlparser.LessThanOp, sqlparser.LessEqualOp:
		default:
			return timeRangeBounds{}, false, nil
		}
		value, err := timeFromVitessValue(e.Right, vars, cfg.Location)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		switch e.Operator {
		case sqlparser.GreaterThanOp:
			return timeRangeBounds{start: value, startExclusive: true}, true, nil
		case sqlparser.GreaterEqualOp:
			return timeRangeBounds{start: value}, true, nil
		case sqlparser.LessThanOp:
			return timeRangeBounds{end: value, endExclusive: true}, true, nil
		case sqlparser.LessEqualOp:
			return timeRangeBounds{end: value}, true, nil
		}
	}
	return timeRangeBounds{}, false, nil
}

// vitessColumnMatches 判断 Vitess AST 中的列是否等于分表字段。
func vitessColumnMatches(expr sqlparser.Expr, key string) bool {
	column, ok := expr.(*sqlparser.ColName)
	return ok && normalizeColumnName(column.Name.String()) == normalizeColumnName(key)
}

// likeRangeBoundsFromVitessExpr 提取分表字段 LIKE 连续日期前缀。
func likeRangeBoundsFromVitessExpr(expr sqlparser.Expr, vars []interface{}, cfg ShardingConfig, key string) (timeRangeBounds, bool, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, leftOK, err := likeRangeBoundsFromVitessExpr(e.Left, vars, cfg, key)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		right, rightOK, err := likeRangeBoundsFromVitessExpr(e.Right, vars, cfg, key)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		if !leftOK {
			return right, rightOK, nil
		}
		if !rightOK {
			return left, true, nil
		}
		return intersectRangeBounds(left, right), true, nil
	case *sqlparser.ComparisonExpr:
		if !vitessColumnMatches(e.Left, key) {
			return timeRangeBounds{}, false, nil
		}
		if e.Operator != sqlparser.LikeOp {
			return timeRangeBounds{}, false, nil
		}
		pattern, err := stringFromVitessValue(e.Right, vars)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		start, end, err := parseShardingLikePrefix(pattern, cfg.Location)
		if err != nil {
			return timeRangeBounds{}, false, err
		}
		return timeRangeBounds{start: start, end: end, endExclusive: true}, true, nil
	}
	return timeRangeBounds{}, false, nil
}

// timeValuesFromVitessValue 从 AST 值表达式中读取一个或多个绑定时间参数。
func timeValuesFromVitessValue(expr sqlparser.Expr, vars []interface{}, location *time.Location) ([]time.Time, bool, error) {
	if argument, ok := expr.(*sqlparser.Argument); ok {
		if index, ok := vitessArgumentIndex(argument, len(vars)); ok {
			values, normalized, err := normalizeShardingClauseValues(vars[index], location)
			if err != nil {
				return nil, false, err
			}
			vars[index] = normalized
			return values, true, nil
		}
		return nil, false, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
	}
	if value, err := timeFromVitessValue(expr, vars, location); err == nil {
		return []time.Time{value}, true, nil
	} else if !isUnsupportedVitessValue(expr) {
		return nil, false, err
	}
	tuple, ok := expr.(sqlparser.ValTuple)
	if !ok {
		if isInlineLiteral(expr) {
			return nil, false, fmt.Errorf("gorm_sharding: inline sharding time literal is not supported")
		}
		return nil, false, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
	}
	out := make([]time.Time, 0, len(tuple))
	for _, item := range tuple {
		values, ok, err := timeValuesFromVitessValue(item, vars, location)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
		}
		out = append(out, values...)
	}
	return out, len(out) > 0, nil
}

// timeFromVitessValue 根据 Vitess 自动生成的 v1、v2 占位符读取 GORM 参数。
func timeFromVitessValue(expr sqlparser.Expr, vars []interface{}, location *time.Location) (time.Time, error) {
	argument, ok := expr.(*sqlparser.Argument)
	if !ok {
		if isInlineLiteral(expr) {
			return time.Time{}, fmt.Errorf("gorm_sharding: inline sharding time literal is not supported")
		}
		return time.Time{}, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
	}
	index, ok := vitessArgumentIndex(argument, len(vars))
	if !ok {
		return time.Time{}, fmt.Errorf("gorm_sharding: unsupported sharding time expression")
	}
	value, err := normalizeShardingClauseValue(vars[index], location)
	if err != nil {
		return time.Time{}, err
	}
	vars[index] = value
	return value, nil
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

// normalizeShardingClauseValue 只用于已经确认属于 ShardingKey 的条件参数。
func normalizeShardingClauseValue(value interface{}, location *time.Location) (time.Time, error) {
	return parseShardingTime(value, effectiveShardingLocation(location))
}

// normalizeShardingClauseValues 处理 ShardingKey 的 IN 参数，支持单个切片或数组参数。
func normalizeShardingClauseValues(value interface{}, location *time.Location) ([]time.Time, interface{}, error) {
	if at, err := normalizeShardingClauseValue(value, location); err == nil {
		return []time.Time{at}, at, nil
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, nil, fmt.Errorf("gorm_sharding: sharding time must be time.Time or supported string")
	}
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil, nil, fmt.Errorf("gorm_sharding: sharding time must be time.Time or supported string")
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, nil, fmt.Errorf("gorm_sharding: sharding time must be time.Time or supported string")
	}
	values := make([]time.Time, 0, rv.Len())
	normalized := make([]interface{}, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		at, err := normalizeShardingClauseValue(rv.Index(i).Interface(), location)
		if err != nil {
			return nil, nil, err
		}
		values = append(values, at)
		normalized = append(normalized, at)
	}
	return values, normalized, nil
}

// stringFromVitessValue 读取 LIKE 的绑定字符串；内联 LIKE 字面量同样拒绝。
func stringFromVitessValue(expr sqlparser.Expr, vars []interface{}) (string, error) {
	argument, ok := expr.(*sqlparser.Argument)
	if !ok {
		if isInlineLiteral(expr) {
			return "", fmt.Errorf("gorm_sharding: inline sharding time literal is not supported")
		}
		return "", fmt.Errorf("gorm_sharding: unsupported sharding time expression")
	}
	index, ok := vitessArgumentIndex(argument, len(vars))
	if !ok {
		return "", fmt.Errorf("gorm_sharding: unsupported sharding time expression")
	}
	pattern, ok := vars[index].(string)
	if !ok {
		return "", fmt.Errorf("gorm_sharding: unsupported sharding time LIKE pattern")
	}
	return pattern, nil
}

func effectiveShardingLocation(location *time.Location) *time.Location {
	if location == nil {
		return time.Local
	}
	return location
}

func isInlineLiteral(expr sqlparser.Expr) bool {
	_, ok := expr.(*sqlparser.Literal)
	return ok
}

func isUnsupportedVitessValue(expr sqlparser.Expr) bool {
	if _, ok := expr.(sqlparser.ValTuple); ok {
		return true
	}
	return isInlineLiteral(expr)
}

func vitessExprReferencesColumn(expr sqlparser.Expr, key string) bool {
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		column, ok := node.(*sqlparser.ColName)
		if ok && normalizeColumnName(column.Name.String()) == normalizeColumnName(key) {
			found = true
			return false, nil
		}
		return true, nil
	}, expr)
	return found
}

func sqlMentionsColumn(sql, key string) bool {
	if sqlExprReferencesColumn(sql, key) {
		return true
	}
	return strings.Contains(strings.ToLower(sql), strings.ToLower(key))
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
	start, end := cfg.shardTime(bounds.start), cfg.shardTime(bounds.end)
	if end.Before(start) || (end.Equal(start) && (bounds.startExclusive || bounds.endExclusive)) {
		// 原始 WHERE 条件必定为空集，不能交换边界后扫描无关的历史分表。
		return nil
	}
	if bounds.endExclusive && cfg.tableName(end) != cfg.tableName(end.Add(-time.Nanosecond)) {
		// 上界恰好位于下一个分片起点时，该分片不属于 [start, end)。
		end = end.Add(-time.Nanosecond)
	}
	if end.Before(start) {
		// 排他上界跨越分片边界时会回退一个纳秒；回退后也可能形成空集。
		return nil
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
