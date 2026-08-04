package gorm_sharding

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"vitess.io/vitess/go/vt/sqlparser"
)

// rewriteRawSQL 解析 Raw SQL 并在 AST 层改写注册过的逻辑表名。
func (p *Plugin) rewriteRawSQL(db *gorm.DB) (string, bool, error) {
	sql := db.Statement.SQL.String()
	// Raw SQL 必须走 AST 解析和回写，避免正则或字符串替换误伤字段、别名、字面量。
	stmt, err := sqlparser.NewTestParser().Parse(sql)
	if err != nil {
		return "", false, err
	}

	changed, err := p.rewriteStatementTable(stmt)
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
func (p *Plugin) rewriteStatementTable(stmt sqlparser.Statement) (bool, error) {
	// v1 只改写单表 SELECT/INSERT/UPDATE/DELETE 的主表名；Join 和复杂子查询先保持原样。
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return p.rewriteTableExprs(s.From)
	case *sqlparser.Insert:
		return p.rewriteAliasedTable(s.Table)
	case *sqlparser.Update:
		return p.rewriteTableExprs(s.TableExprs)
	case *sqlparser.Delete:
		return p.rewriteTableExprs(s.TableExprs)
	default:
		return false, nil
	}
}

// rewriteTableExprs 改写 SELECT/UPDATE/DELETE 中的单个 table expression。
func (p *Plugin) rewriteTableExprs(exprs []sqlparser.TableExpr) (bool, error) {
	if len(exprs) != 1 {
		return false, nil
	}
	aliased, ok := exprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return false, nil
	}
	return p.rewriteAliasedTable(aliased)
}

// rewriteAliasedTable 改写带别名的表表达式中的真实表名。
func (p *Plugin) rewriteAliasedTable(aliased *sqlparser.AliasedTableExpr) (bool, error) {
	if aliased == nil {
		return false, nil
	}
	table, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return false, nil
	}
	changed, err := p.rewriteTableName(&table)
	if changed {
		aliased.Expr = table
	}
	return changed, err
}

// rewriteTableName 把 AST 中的逻辑表名替换成当前策略选出的真实分表名。
func (p *Plugin) rewriteTableName(table *sqlparser.TableName) (bool, error) {
	name := table.Name.String()
	cfg, ok := p.configByPrefix(name)
	if !ok {
		return false, nil
	}
	// Raw 无法可靠读取绑定变量里的分表时间，第一版按最近表扫描策略取首张目标表。
	targets := p.manager.tables(cfg, time.Now())
	if len(targets) == 0 {
		return false, fmt.Errorf("gorm_sharding: no target table for %s", name)
	}
	table.Name = sqlparser.NewIdentifierCS(targets[0])
	return true, nil
}

// configByPrefix 根据 Raw SQL 中出现的逻辑表名找到分表配置。
func (p *Plugin) configByPrefix(name string) (ShardingConfig, bool) {
	name = strings.Trim(name, "`")
	for _, cfg := range p.configs {
		if cfg.TablePrefix == name {
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
