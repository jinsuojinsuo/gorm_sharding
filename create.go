package gorm_sharding

import (
	"fmt"
	"reflect"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// groupCreateValues 按分表字段把 Create 的单条或批量数据分组到目标真实表。
func (p *Plugin) groupCreateValues(db *gorm.DB, cfg ShardingConfig) (map[string]reflect.Value, error) {
	value := db.Statement.ReflectValue
	if !value.IsValid() {
		value = reflect.ValueOf(db.Statement.Dest)
	}
	for value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		t, ok := valueTime(value, db.Statement.Schema, cfg.ShardingKey)
		if !ok {
			return nil, fmt.Errorf("gorm_sharding: sharding key %s must be time.Time", cfg.ShardingKey)
		}
		// 单条插入也统一返回分组结构，调用方可以共用建表和路由逻辑。
		return map[string]reflect.Value{cfg.tableName(t): value.Addr()}, nil
	}

	groups := make(map[string]reflect.Value)
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i)
		t, ok := valueTime(elem, db.Statement.Schema, cfg.ShardingKey)
		if !ok {
			return nil, fmt.Errorf("gorm_sharding: sharding key %s must be time.Time", cfg.ShardingKey)
		}
		table := cfg.tableName(t)
		group, ok := groups[table]
		if !ok {
			group = reflect.MakeSlice(value.Type(), 0, 1)
		}
		// 同一批数据按目标分表聚合，后续每个分表执行一次批量插入。
		groups[table] = reflect.Append(group, elem)
	}
	return groups, nil
}

// valueTime 从结构体中读取分表字段的 time.Time 值。
func valueTime(v reflect.Value, s *schema.Schema, key string) (time.Time, bool) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return time.Time{}, false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return time.Time{}, false
	}
	fieldName := key
	if s != nil {
		if f := s.LookUpField(key); f != nil {
			// ShardingKey 通常写数据库列名，例如 created_at；这里映射回 Go 字段名 CreatedAt。
			fieldName = f.Name
		}
	}
	f := v.FieldByName(fieldName)
	if !f.IsValid() {
		return time.Time{}, false
	}
	t, ok := f.Interface().(time.Time)
	return t, ok && !t.IsZero()
}
