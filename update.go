package gorm_sharding

import (
	"fmt"
	"reflect"

	"gorm.io/gorm"
)

// groupUpdateValues 按分表字段把批量 Updates 的模型实体分组到对应真实分表。
// grouped 为 false 表示当前 Updates 的模型不是非空切片或数组，应继续使用普通条件路由。
func (p *Plugin) groupUpdateValues(db *gorm.DB, cfg ShardingConfig) (groups map[string]reflect.Value, grouped bool, err error) {
	value := db.Statement.ReflectValue
	if !value.IsValid() {
		value = reflect.ValueOf(db.Statement.Model)
	}
	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil, false, nil
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return nil, false, nil
	}
	if value.Len() == 0 {
		return nil, false, nil
	}

	groups = make(map[string]reflect.Value)
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i)
		at, ok := valueTime(elem, db.Statement.Schema, cfg.ShardingKey)
		if !ok {
			return nil, true, fmt.Errorf("gorm_sharding: sharding key %s must be time.Time", cfg.ShardingKey)
		}
		table := cfg.tableName(at)
		group, ok := groups[table]
		if !ok {
			group = reflect.MakeSlice(groupSliceType(value), 0, 1)
		}
		groups[table] = reflect.Append(group, elem)
	}
	return groups, true, nil
}

// updateShardValues 更新一个真实分表中的一组实体，并保留调用方已有的 WHERE、Select、Omit 等状态。
func (p *Plugin) updateShardValues(db *gorm.DB, table string, values interface{}) *gorm.DB {
	tx := db.Session(&gorm.Session{NewDB: true, SkipHooks: true}).Table(table)
	copyWriteState(tx, db)
	tx = tx.Set(skipKey, true)
	return tx.Model(values).Updates(db.Statement.Dest)
}
