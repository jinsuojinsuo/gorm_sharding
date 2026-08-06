package gorm_sharding

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"gorm.io/gorm"
	"vitess.io/vitess/go/vt/sqlparser"
)

// executeRawWriteAcrossShards 逐表执行命中多张分表的 Raw UPDATE 或 DELETE。
// 调用方已开启事务时复用当前事务；否则创建内部事务，保证任一分表失败时不会部分成功。
func (p *Plugin) executeRawWriteAcrossShards(db *gorm.DB) (bool, error) {
	stmt, err := sqlparser.NewTestParser().Parse(db.Statement.SQL.String())
	if err != nil {
		return false, nil
	}
	cfg, targets, ok, err := p.rawWriteTargets(stmt, db.Statement.Vars)
	if err != nil || !ok || len(targets) <= 1 {
		return ok && err != nil, err
	}
	if rawWriteHasLimit(stmt) {
		// 多分表逐表执行 LIMIT 会把单表的全局限制放大为“每张表各 LIMIT 一次”，
		// 无法保持单逻辑表的 UPDATE/DELETE 语义，必须明确拒绝。
		return true, fmt.Errorf("gorm_sharding: limit across shards is not supported")
	}
	vars := append([]interface{}(nil), db.Statement.Vars...)

	execute := func(tx *gorm.DB) (int64, error) {
		var rows int64
		for _, table := range targets {
			sql, err := rawWriteSQLForTable(stmt, table)
			if err != nil {
				return 0, err
			}
			// 显式传入 Context 会让 GORM 复制 Statement，避免清空内部 SQL 时
			// 修改外层事务正在使用的 Statement 和参数。
			shard := tx.Session(&gorm.Session{Context: tx.Statement.Context})
			shard.Statement.SQL.Reset()
			shard.Statement.Vars = nil
			shard.Statement.SQL.WriteString(sql)
			shard.Statement.Vars = append([]interface{}(nil), vars...)
			p.rawFn(shard)
			if shard.Error != nil {
				if isMissingTableError(shard.Error) {
					p.manager.invalidate(cfg, table)
					// 缺失分表按空表处理，继续执行其他命中的真实分表。
					continue
				}
				return 0, shard.Error
			}
			rows += shard.RowsAffected
		}
		return rows, nil
	}

	var rows int64
	if hasActiveTransaction(db) {
		rows, err = execute(db)
	} else {
		err = db.Transaction(func(tx *gorm.DB) error {
			var executeErr error
			rows, executeErr = execute(tx)
			return executeErr
		})
	}
	if err != nil {
		return true, err
	}
	db.RowsAffected = rows
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = rows
	}
	return true, nil
}

// rawWriteHasLimit 判断 Raw UPDATE 或 DELETE 是否携带 LIMIT 子句。
func rawWriteHasLimit(stmt sqlparser.Statement) bool {
	switch statement := stmt.(type) {
	case *sqlparser.Update:
		return statement.Limit != nil
	case *sqlparser.Delete:
		return statement.Limit != nil
	default:
		return false
	}
}

// rawWriteTargets 返回 Raw UPDATE 或 DELETE 的分表配置和所有目标真实表。
func (p *Plugin) rawWriteTargets(stmt sqlparser.Statement, vars []interface{}) (ShardingConfig, []string, bool, error) {
	var table sqlparser.TableName
	var update *sqlparser.Update
	switch statement := stmt.(type) {
	case *sqlparser.Update:
		if len(statement.TableExprs) != 1 {
			return p.rejectComplexRawWrite(stmt)
		}
		aliased, ok := statement.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok {
			return p.rejectComplexRawWrite(stmt)
		}
		var tableOK bool
		table, tableOK = aliased.Expr.(sqlparser.TableName)
		if !tableOK {
			return p.rejectComplexRawWrite(stmt)
		}
		update = statement
	case *sqlparser.Delete:
		if len(statement.TableExprs) != 1 {
			return p.rejectComplexRawWrite(stmt)
		}
		aliased, ok := statement.TableExprs[0].(*sqlparser.AliasedTableExpr)
		if !ok {
			return p.rejectComplexRawWrite(stmt)
		}
		var tableOK bool
		table, tableOK = aliased.Expr.(sqlparser.TableName)
		if !tableOK {
			return p.rejectComplexRawWrite(stmt)
		}
	default:
		return ShardingConfig{}, nil, false, nil
	}

	cfg, ok := p.configByPrefix(table.Name.String())
	if !ok {
		return ShardingConfig{}, nil, false, nil
	}
	if update != nil && rawUpdateChangesShardingKey(update, cfg.ShardingKey) {
		return ShardingConfig{}, nil, true, fmt.Errorf("gorm_sharding: updating sharding key %s is not supported", cfg.ShardingKey)
	}
	targets, routed := rawStatementTables(stmt, vars, cfg, cfg.ShardingKey)
	if !routed {
		targets = p.manager.tables(cfg, time.Now())
	}
	if len(targets) == 0 {
		return ShardingConfig{}, nil, true, fmt.Errorf("gorm_sharding: no target table for %s", table.Name.String())
	}
	return cfg, targets, true, nil
}

// rejectComplexRawWrite 拒绝引用逻辑分表的 JOIN、多表或派生表 Raw 写入。
// 这类 SQL 不能安全地拆成逐分表语句，绝不能回退执行未改写的逻辑表名。
func (p *Plugin) rejectComplexRawWrite(stmt sqlparser.Statement) (ShardingConfig, []string, bool, error) {
	if p.rawStatementReferencesShardedTable(stmt) {
		return ShardingConfig{}, nil, true, fmt.Errorf("gorm_sharding: raw multi-table write is not supported")
	}
	return ShardingConfig{}, nil, false, nil
}

// rawStatementReferencesShardedTable 判断 AST 任意位置是否引用已注册的逻辑分表。
func (p *Plugin) rawStatementReferencesShardedTable(stmt sqlparser.Statement) bool {
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		table, ok := node.(sqlparser.TableName)
		if !ok {
			return true, nil
		}
		if _, ok := p.configByPrefix(table.Name.String()); ok {
			found = true
			return false, nil
		}
		return true, nil
	}, stmt)
	return found
}

// rawWriteSQLForTable 克隆 Raw 写入 AST 并把唯一逻辑表改写为指定真实分表。
func rawWriteSQLForTable(stmt sqlparser.Statement, table string) (string, error) {
	clone := sqlparser.CloneStatement(stmt)
	var aliased *sqlparser.AliasedTableExpr
	switch statement := clone.(type) {
	case *sqlparser.Update:
		aliased, _ = statement.TableExprs[0].(*sqlparser.AliasedTableExpr)
	case *sqlparser.Delete:
		aliased, _ = statement.TableExprs[0].(*sqlparser.AliasedTableExpr)
	}
	if aliased == nil {
		return "", fmt.Errorf("gorm_sharding: raw write must target one table")
	}
	aliased.Expr = sqlparser.TableName{Name: sqlparser.NewIdentifierCS(table)}
	parsed := sqlparser.NewParsedQuery(clone)
	return restorePositionalBindVars(parsed.Query, parsed.BindLocations()), nil
}

// hasActiveTransaction 判断当前 GORM 连接池是否已处于事务中。
func hasActiveTransaction(db *gorm.DB) bool {
	committer, ok := db.Statement.ConnPool.(gorm.TxCommitter)
	if !ok || committer == nil {
		return false
	}
	value := reflect.ValueOf(committer)
	return value.Kind() != reflect.Ptr || !value.IsNil()
}

// rewriteRawSQL 解析 Raw SQL 并在 AST 层改写注册过的逻辑表名。
func (p *Plugin) rewriteRawSQL(db *gorm.DB) (string, bool, error) {
	sql := db.Statement.SQL.String()
	// Raw SQL 必须走 AST 解析和回写，避免正则或字符串替换误伤字段、别名、字面量。
	stmt, err := sqlparser.NewTestParser().Parse(sql)
	if err != nil {
		return "", false, err
	}

	changed, err := p.rewriteStatementTable(stmt, db.Statement.Vars)
	if err != nil {
		return "", false, err
	}
	if !changed {
		return "", false, nil
	}
	parsed := sqlparser.NewParsedQuery(stmt)
	return restorePositionalBindVars(parsed.Query, parsed.BindLocations()), true, nil
}

// rewriteStatementTable 根据 SQL 语句类型定位需要改写的主表。
func (p *Plugin) rewriteStatementTable(stmt sqlparser.Statement, vars []interface{}) (bool, error) {
	// v1 只改写单表 SELECT/INSERT/UPDATE/DELETE 的主表名；Join 和复杂子查询先保持原样。
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return p.rewriteTableExprs(s.From, stmt, vars)
	case *sqlparser.Insert:
		return p.rewriteAliasedTable(s.Table, stmt, vars)
	case *sqlparser.Update:
		return p.rewriteTableExprs(s.TableExprs, stmt, vars)
	case *sqlparser.Delete:
		return p.rewriteTableExprs(s.TableExprs, stmt, vars)
	default:
		return false, nil
	}
}

// rewriteTableExprs 改写 SELECT/UPDATE/DELETE 中的单个 table expression。
func (p *Plugin) rewriteTableExprs(exprs []sqlparser.TableExpr, stmt sqlparser.Statement, vars []interface{}) (bool, error) {
	if len(exprs) != 1 {
		return false, nil
	}
	aliased, ok := exprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return false, nil
	}
	return p.rewriteAliasedTable(aliased, stmt, vars)
}

// rewriteAliasedTable 改写带别名的表表达式中的真实表名。
func (p *Plugin) rewriteAliasedTable(aliased *sqlparser.AliasedTableExpr, stmt sqlparser.Statement, vars []interface{}) (bool, error) {
	if aliased == nil {
		return false, nil
	}
	table, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return false, nil
	}
	changed, err := p.rewriteTableName(&table, stmt, vars)
	if changed {
		aliased.Expr = table
	}
	return changed, err
}

// rewriteTableName 把 AST 中的逻辑表名替换成当前策略选出的真实分表名。
func (p *Plugin) rewriteTableName(table *sqlparser.TableName, stmt sqlparser.Statement, vars []interface{}) (bool, error) {
	name := table.Name.String()
	cfg, ok := p.configByPrefix(name)
	if !ok {
		return false, nil
	}
	if _, ok := stmt.(*sqlparser.Insert); ok {
		// INSERT 的分表字段位于 VALUES，当前 Raw 路由不会解析值列表，禁止静默写入最新分表。
		return false, fmt.Errorf("gorm_sharding: raw insert into sharded table is not supported; use Create")
	}
	if update, ok := stmt.(*sqlparser.Update); ok && rawUpdateChangesShardingKey(update, cfg.ShardingKey) {
		return false, fmt.Errorf("gorm_sharding: updating sharding key %s is not supported", cfg.ShardingKey)
	}
	targets, routed := rawStatementTables(stmt, vars, cfg, cfg.ShardingKey)
	if routed && len(targets) != 1 {
		return false, fmt.Errorf("gorm_sharding: raw SQL across shards is not supported")
	}
	if !routed {
		// 无分表字段条件时保留 Raw 单表默认查询最新真实分表的行为。
		targets = p.manager.tables(cfg, time.Now())
	}
	if len(targets) == 0 {
		return false, fmt.Errorf("gorm_sharding: no target table for %s", name)
	}
	table.Name = sqlparser.NewIdentifierCS(targets[0])
	return true, nil
}

// rawUpdateChangesShardingKey 判断 Raw UPDATE 的 SET 子句是否修改了分表字段。
func rawUpdateChangesShardingKey(update *sqlparser.Update, key string) bool {
	for _, assignment := range update.Exprs {
		if assignment != nil && assignment.Name != nil && normalizeColumnName(assignment.Name.Name.String()) == normalizeColumnName(key) {
			return true
		}
	}
	return false
}

// configByPrefix 根据 Raw SQL 中出现的逻辑表名找到分表配置。
func (p *Plugin) configByPrefix(name string) (ShardingConfig, bool) {
	name = strings.Trim(name, "`")
	for _, cfg := range p.configs {
		if cfg.tablePrefix == name {
			return cfg, true
		}
	}
	return ShardingConfig{}, false
}

// restorePositionalBindVars 把 Vitess 输出的 bind location 恢复为 MySQL driver 使用的 ? 占位符。
func restorePositionalBindVars(sql string, locations []sqlparser.BindLocation) string {
	if len(locations) == 0 {
		return sql
	}
	var builder strings.Builder
	current := 0
	for _, location := range locations {
		builder.WriteString(sql[current:location.Offset])
		builder.WriteByte('?')
		current = location.Offset + location.Length
	}
	builder.WriteString(sql[current:])
	return builder.String()
}
