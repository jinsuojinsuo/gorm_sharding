package gorm_sharding

import (
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"vitess.io/vitess/go/vt/sqlparser"
)

// updatesShardingKey 判断当前 GORM Update/Save 是否会写入分表字段。
func updatesShardingKey(db *gorm.DB, cfg ShardingConfig) bool {
	if shardingKeyOmitted(db, db.Statement.Omits, cfg.ShardingKey) {
		return false
	}
	if len(db.Statement.Selects) > 0 {
		for _, field := range db.Statement.Selects {
			if field == "*" || statementFieldMatches(db, field, cfg.ShardingKey) {
				return true
			}
		}
		return false
	}

	value := reflect.ValueOf(db.Statement.Dest)
	for value.IsValid() && value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return false
	}
	if value.Kind() == reflect.Map && value.Type().Key().Kind() == reflect.String {
		iter := value.MapRange()
		for iter.Next() {
			if statementFieldMatches(db, iter.Key().String(), cfg.ShardingKey) {
				return true
			}
		}
		return false
	}
	if value.Kind() != reflect.Struct || db.Statement.Schema == nil {
		return false
	}
	field := db.Statement.Schema.LookUpField(cfg.ShardingKey)
	if field == nil {
		return false
	}
	_, zero := field.ValueOf(db.Statement.Context, value)
	return !zero
}

// createIncludesShardingKey 判断 Create 的实际插入列是否包含分表字段。
func createIncludesShardingKey(db *gorm.DB, cfg ShardingConfig) bool {
	if shardingKeyOmitted(db, db.Statement.Omits, cfg.ShardingKey) {
		return false
	}
	if len(db.Statement.Selects) == 0 {
		return true
	}
	for _, field := range db.Statement.Selects {
		if field == "*" || statementFieldMatches(db, field, cfg.ShardingKey) {
			return true
		}
	}
	return false
}

// createUpdatesShardingKey 判断 Create 的 ON CONFLICT 分支是否可能修改分表字段。
func createUpdatesShardingKey(db *gorm.DB, cfg ShardingConfig) bool {
	clauseValue, ok := db.Statement.Clauses["ON CONFLICT"]
	if !ok {
		return false
	}
	conflict, ok := clauseValue.Expression.(clause.OnConflict)
	if !ok || conflict.DoNothing {
		return false
	}
	if conflict.UpdateAll {
		return true
	}
	for _, assignment := range conflict.DoUpdates {
		if statementFieldMatches(db, assignment.Column.Name, cfg.ShardingKey) {
			return true
		}
	}
	return false
}

// shardingKeyOmitted 判断 GORM Omit 设置是否排除了分表字段。
func shardingKeyOmitted(db *gorm.DB, omits []string, key string) bool {
	for _, field := range omits {
		if strings.EqualFold(field, "*") || statementFieldMatches(db, field, key) {
			return true
		}
	}
	return false
}

// statementFieldMatches 同时识别 GORM 支持的 Go 字段名和数据库列名。
func statementFieldMatches(db *gorm.DB, name, key string) bool {
	if normalizeColumnName(name) == normalizeColumnName(key) {
		return true
	}
	if db.Statement.Schema == nil {
		return false
	}
	field := db.Statement.Schema.LookUpField(normalizeColumnName(name))
	return field != nil && normalizeColumnName(field.DBName) == normalizeColumnName(key)
}

// primaryKeyWriteWithoutShardingKey 判断按主键写入是否缺少完整分表定位信息。
// 分表间自增主键可能重复，因此不能把仅含主键的 Update/Delete 扫描到多张真实分表。
func primaryKeyWriteWithoutShardingKey(db *gorm.DB, cfg ShardingConfig) bool {
	if hasWriteShardingRoute(db, cfg) {
		return false
	}
	primary := statementPrimaryKey(db)
	if primary == "" {
		return false
	}
	if statementHasPrimaryKeyValue(db) {
		return true
	}
	where, ok := db.Statement.Clauses["WHERE"].Expression.(clause.Where)
	return ok && exprsReferenceColumn(where.Exprs, primary)
}

// hasWriteShardingRoute 判断当前 Update/Delete 是否能从模型或 WHERE 中得到完整分表列表。
func hasWriteShardingRoute(db *gorm.DB, cfg ShardingConfig) bool {
	if _, ok := timeFromReflect(db.Statement.ReflectValue, db.Statement.Schema, cfg.ShardingKey); ok {
		return true
	}
	where, ok := db.Statement.Clauses["WHERE"].Expression.(clause.Where)
	if !ok {
		return false
	}
	_, ok = tablesFromExprs(where.Exprs, cfg, cfg.ShardingKey)
	return ok
}

// statementPrimaryKey 返回当前模型主键的数据库列名。
func statementPrimaryKey(db *gorm.DB) string {
	if db.Statement.Schema == nil || db.Statement.Schema.PrioritizedPrimaryField == nil {
		return ""
	}
	return db.Statement.Schema.PrioritizedPrimaryField.DBName
}

// statementHasPrimaryKeyValue 判断模型实体是否携带非零主键。
func statementHasPrimaryKeyValue(db *gorm.DB) bool {
	value := db.Statement.ReflectValue
	if !value.IsValid() {
		return false
	}
	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		for i := 0; i < value.Len(); i++ {
			if statementValueHasPrimaryKey(db, value.Index(i)) {
				return true
			}
		}
		return false
	}
	return statementValueHasPrimaryKey(db, value)
}

// statementValueHasPrimaryKey 判断单个模型值是否携带非零主键。
func statementValueHasPrimaryKey(db *gorm.DB, value reflect.Value) bool {
	primary := db.Statement.Schema.PrioritizedPrimaryField
	_, zero := primary.ValueOf(db.Statement.Context, value)
	return !zero
}

// exprsReferenceColumn 判断 GORM WHERE 表达式是否引用指定数据库列。
func exprsReferenceColumn(exprs []clause.Expression, column string) bool {
	for _, expr := range exprs {
		switch condition := expr.(type) {
		case clause.Eq:
			if columnMatches(condition.Column, column) {
				return true
			}
		case clause.IN:
			if columnMatches(condition.Column, column) {
				return true
			}
		case clause.AndConditions:
			if exprsReferenceColumn(condition.Exprs, column) {
				return true
			}
		case clause.OrConditions:
			if exprsReferenceColumn(condition.Exprs, column) {
				return true
			}
		case clause.NotConditions:
			if exprsReferenceColumn(condition.Exprs, column) {
				return true
			}
		case clause.Expr:
			if sqlExprReferencesColumn(condition.SQL, column) {
				return true
			}
		}
	}
	return false
}

// sqlExprReferencesColumn 使用 Vitess AST 判断字符串 WHERE 条件是否引用指定列。
func sqlExprReferencesColumn(sql, column string) bool {
	expr, ok := parseConditionSQL(sql)
	if !ok {
		return false
	}
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		name, ok := node.(*sqlparser.ColName)
		if ok && normalizeColumnName(name.Name.String()) == normalizeColumnName(column) {
			found = true
			return false, nil
		}
		return true, nil
	}, expr)
	return found
}
