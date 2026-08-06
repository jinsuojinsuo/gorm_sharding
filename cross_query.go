package gorm_sharding

import (
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"vitess.io/vitess/go/vt/sqlparser"
)

// executeCombinedQuery 使用 UNION ALL 在 MySQL 内完成跨分表排序、分页和分组聚合。
func (p *Plugin) executeCombinedQuery(db *gorm.DB, cfg ShardingConfig, tables []string) error {
	return p.executeCombined(db, cfg, tables, p.queryFn)
}

// executeCombinedRow 使用 UNION ALL 执行跨分表 Scan、Rows 或 Row 回调支持的聚合查询。
func (p *Plugin) executeCombinedRow(db *gorm.DB, cfg ShardingConfig, tables []string) error {
	return p.executeCombined(db, cfg, tables, p.rowFn)
}

// executeCombined 执行组合 SQL；首次执行返回 1146 时才检查候选表并至多重试一次。
func (p *Plugin) executeCombined(db *gorm.DB, cfg ShardingConfig, tables []string, execute func(*gorm.DB)) error {
	run := func(targets []string) error {
		sql, vars, err := p.buildCombinedQuery(db, targets)
		if err != nil {
			return err
		}
		db.Statement.SQL.Reset()
		db.Statement.SQL.WriteString(sql)
		db.Statement.Vars = vars
		execute(db)
		return db.Error
	}

	err := run(tables)
	if !isMissingTableError(err) {
		return err
	}

	// 遵循“先执行 SQL”的策略：只有 MySQL 返回 1146 后才读取元数据并剔除已不存在的分表。
	retryTables := p.manager.existingAfterMissing(db, cfg, tables)
	if len(retryTables) == len(tables) {
		return err
	}
	db.Error = nil
	db.RowsAffected = 0
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = 0
	}
	if len(retryTables) == 0 {
		return nil
	}
	if len(retryTables) == 1 {
		// 组合查询退化为单表后交回原始回调，避免 buildCombinedQuery 拒绝单表输入。
		db.Statement.SQL.Reset()
		db.Statement.Vars = nil
		setStatementTable(db, retryTables[0])
		execute(db)
		return db.Error
	}
	return run(retryTables)
}

// buildCombinedQuery 把 GORM 单表 SELECT 改写为“内层分表 UNION ALL，外层保留原查询语义”的 SQL。
func (p *Plugin) buildCombinedQuery(db *gorm.DB, tables []string) (string, []interface{}, error) {
	if len(tables) < 2 {
		return "", nil, fmt.Errorf("gorm_sharding: combined query requires multiple shards")
	}

	// 先让 GORM 在 DryRun 模式构建原始 SELECT，保留 Where、Select、Group、Having 等语义。
	dryRun := db.Session(&gorm.Session{NewDB: true, DryRun: true}).Table(tables[0])
	copyQueryState(dryRun, db)
	dryRun = dryRun.Set(skipKey, true).Find(db.Statement.Dest)
	if dryRun.Error != nil {
		return "", nil, dryRun.Error
	}
	stmt, err := sqlparser.NewTestParser().Parse(dryRun.Statement.SQL.String())
	if err != nil {
		return "", nil, err
	}
	original, ok := stmt.(*sqlparser.Select)
	if !ok || len(original.From) != 1 {
		return "", nil, fmt.Errorf("gorm_sharding: cross shard query must be a single table select")
	}
	if selectHasSubquery(original) {
		return "", nil, fmt.Errorf("gorm_sharding: subquery across shards is not supported")
	}

	outer := sqlparser.CloneRefOfSelect(original)
	innerTemplate := sqlparser.CloneRefOfSelect(original)
	// 内层只提供原始行；Group By、聚合、Having 必须留给外层一次性执行，才能得到全局结果。
	innerTemplate.SelectExprs = &sqlparser.SelectExprs{Exprs: []sqlparser.SelectExpr{&sqlparser.StarExpr{}}}
	innerTemplate.GroupBy = nil
	innerTemplate.Having = nil
	innerTemplate.OrderBy = nil
	innerTemplate.Limit = nil
	innerTemplate.Distinct = false

	if limit, offset, ok := statementLimit(db); ok {
		// 只有普通明细查询才能把分页下推到每张分表。DISTINCT、聚合、分组和
		// HAVING 都必须先基于完整原始行集在外层计算，否则会改变单表 SQL 语义。
		if canPushDownDetailLimit(db, outer) {
			innerTemplate.OrderBy = sqlparser.CloneOrderBy(original.OrderBy)
			innerTemplate.Limit = sqlLimit(limit + offset)
		}
		outer.Limit = sqlLimitWithOffset(limit, offset)
	}

	var union sqlparser.TableStatement
	for _, table := range tables {
		inner := sqlparser.CloneRefOfSelect(innerTemplate)
		setSelectTable(inner, table)
		if union == nil {
			union = inner
			continue
		}
		union = &sqlparser.Union{Left: union, Right: inner}
	}

	// 外层复用原 Select、Group By、Having、Order By 和 Limit，只将来源替换成分表行集合。
	outer.Where = nil
	outer.From = []sqlparser.TableExpr{&sqlparser.AliasedTableExpr{
		Expr: &sqlparser.DerivedTable{Select: union},
		As:   sqlparser.NewIdentifierCS("gorm_sharding_rows"),
	}}
	outer = removeTableQualifiers(outer)
	parsed := sqlparser.NewParsedQuery(outer)
	vars, err := positionalVars(parsed, dryRun.Statement.Vars)
	if err != nil {
		return "", nil, err
	}
	return restorePositionalBindVars(parsed.Query, parsed.BindLocations()), vars, nil
}

// canPushDownDetailLimit 判断当前查询是否可以安全地把分页下推到每张真实分表。
// 对普通明细的全局 Top N，下推 offset + limit 不会遗漏结果；其他查询必须由外层 MySQL
// 在完整行集上执行 DISTINCT、聚合、GROUP BY 或 HAVING，才能保持与单表查询一致。
func canPushDownDetailLimit(db *gorm.DB, selectStmt *sqlparser.Select) bool {
	if db.Statement.Distinct || selectStmt.Distinct || isAggregateQuery(db) {
		return false
	}
	if selectStmt.GroupBy != nil && len(selectStmt.GroupBy.Exprs) > 0 {
		return false
	}
	return selectStmt.Having == nil
}

// statementLimit 读取 GORM 当前查询的分页参数。
func statementLimit(db *gorm.DB) (limit, offset int, ok bool) {
	clauseValue, ok := db.Statement.Clauses["LIMIT"]
	if !ok {
		return 0, 0, false
	}
	limitClause, ok := clauseValue.Expression.(clause.Limit)
	if !ok || limitClause.Limit == nil || *limitClause.Limit < 0 {
		return 0, 0, false
	}
	return *limitClause.Limit, limitClause.Offset, true
}

// isAggregateQuery 判断当前 GORM 查询是否包含需要在全部分表上计算的常用聚合函数。
func isAggregateQuery(db *gorm.DB) bool {
	if selectClause, ok := db.Statement.Clauses["SELECT"]; ok {
		if expression, ok := selectClause.Expression.(clause.Expr); ok && containsAggregate(expression.SQL) {
			return true
		}
	}
	for _, selectExpr := range db.Statement.Selects {
		if containsAggregate(selectExpr) {
			return true
		}
	}
	return false
}

// hasGroupBy 判断当前查询是否包含 GROUP BY。
func hasGroupBy(db *gorm.DB) bool {
	_, ok := db.Statement.Clauses["GROUP BY"]
	return ok
}

// containsAggregate 检查 SQL 选择表达式是否包含本插件支持的常用聚合函数。
func containsAggregate(sql string) bool {
	lower := strings.ToLower(sql)
	for _, name := range []string{"count(", "sum(", "min(", "max(", "avg("} {
		if strings.Contains(lower, name) {
			return true
		}
	}
	return false
}

// sqlLimit 构造无 Offset 的 SQL LIMIT 节点。
func sqlLimit(limit int) *sqlparser.Limit {
	return &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral(strconv.Itoa(limit))}
}

// sqlLimitWithOffset 构造外层统一分页使用的 SQL LIMIT 节点。
func sqlLimitWithOffset(limit, offset int) *sqlparser.Limit {
	result := sqlLimit(limit)
	if offset > 0 {
		result.Offset = sqlparser.NewIntLiteral(strconv.Itoa(offset))
	}
	return result
}

// setSelectTable 把单表 SELECT 的 From 表替换为一个真实分表。
func setSelectTable(selectStmt *sqlparser.Select, table string) {
	aliased := selectStmt.From[0].(*sqlparser.AliasedTableExpr)
	aliased.Expr = sqlparser.TableName{Name: sqlparser.NewIdentifierCS(table)}
}

// removeTableQualifiers 删除外层表达式中原始逻辑表限定名，使其引用派生表列。
func removeTableQualifiers(selectStmt *sqlparser.Select) *sqlparser.Select {
	rewritten := sqlparser.CopyOnRewrite(selectStmt, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		column, ok := cursor.Node().(*sqlparser.ColName)
		if !ok || column.Qualifier.Name.String() == "" {
			return
		}
		copy := *column
		copy.Qualifier = sqlparser.TableName{}
		cursor.Replace(&copy)
	}, nil)
	return rewritten.(*sqlparser.Select)
}

// selectHasSubquery 判断组合查询的原始 Select 是否包含子查询。
// 组合查询会把主表替换为派生表，相关子查询的限定列无法在不改变语义的前提下安全改写。
func selectHasSubquery(selectStmt *sqlparser.Select) bool {
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node.(type) {
		case *sqlparser.Subquery, *sqlparser.DerivedTable:
			found = true
			return false, nil
		}
		return true, nil
	}, selectStmt)
	return found
}

// positionalVars 按 Vitess 输出的占位符顺序复制 GORM 原始参数。
func positionalVars(parsed *sqlparser.ParsedQuery, original []interface{}) ([]interface{}, error) {
	vars := make([]interface{}, 0, len(parsed.BindLocations()))
	for _, location := range parsed.BindLocations() {
		name := strings.TrimPrefix(parsed.Query[location.Offset:location.Offset+location.Length], ":")
		if !strings.HasPrefix(name, "v") {
			return nil, fmt.Errorf("gorm_sharding: unsupported bind variable %s in cross shard query", name)
		}
		index, err := strconv.Atoi(strings.TrimPrefix(name, "v"))
		if err != nil || index <= 0 || index > len(original) {
			return nil, fmt.Errorf("gorm_sharding: invalid bind variable %s in cross shard query", name)
		}
		vars = append(vars, original[index-1])
	}
	return vars, nil
}
