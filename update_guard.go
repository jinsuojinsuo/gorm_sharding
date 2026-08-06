package gorm_sharding

import (
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
