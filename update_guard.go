package gorm_sharding

import (
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// updatesShardingKey 判断当前 GORM Update/Save 是否会写入分表字段。
func updatesShardingKey(db *gorm.DB, cfg ShardingConfig) bool {
	if shardingKeyOmitted(db.Statement.Omits, cfg.ShardingKey) {
		return false
	}
	if len(db.Statement.Selects) > 0 {
		for _, field := range db.Statement.Selects {
			if field == "*" || normalizeColumnName(field) == normalizeColumnName(cfg.ShardingKey) {
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
			if normalizeColumnName(iter.Key().String()) == normalizeColumnName(cfg.ShardingKey) {
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

// shardingKeyOmitted 判断 GORM Omit 设置是否排除了分表字段。
func shardingKeyOmitted(omits []string, key string) bool {
	for _, field := range omits {
		if strings.EqualFold(field, "*") || normalizeColumnName(field) == normalizeColumnName(key) {
			return true
		}
	}
	return false
}
