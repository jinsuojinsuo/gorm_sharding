package gorm_sharding

import (
	"fmt"
	"reflect"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// createValueGroup 保存一个目标分表的插入数据，以及这些数据在调用方原始切片中的下标。
// GORM 会把自增主键等数据库生成字段回填到 Values，需要据此同步回原始 []T。
type createValueGroup struct {
	values        reflect.Value
	original      reflect.Value
	sourceIndexes []int
}

// groupCreateValues 按分表字段把 Create 的单条或批量数据分组到目标真实表。
func (p *Plugin) groupCreateValues(db *gorm.DB, cfg ShardingConfig) (map[string]createValueGroup, error) {
	value := db.Statement.ReflectValue
	if !value.IsValid() {
		value = reflect.ValueOf(db.Statement.Dest)
	}
	for value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		// GORM 原生 Create(User{}) 返回 ErrInvalidValue；插件不能因为调用 Addr 而 panic。
		if !value.CanAddr() {
			return nil, gorm.ErrInvalidValue
		}
		t, ok := valueTime(value, db.Statement.Schema, cfg.ShardingKey)
		if !ok {
			return nil, fmt.Errorf("gorm_sharding: sharding key %s must be time.Time", cfg.ShardingKey)
		}
		// 单条插入也统一返回分组结构，调用方可以共用建表和路由逻辑。
		return map[string]createValueGroup{cfg.tableName(t): {values: value.Addr()}}, nil
	}
	if value.Len() == 0 {
		// GORM 原生 Create 对空切片或空数组返回 ErrEmptySlice。
		return nil, gorm.ErrEmptySlice
	}

	groups := make(map[string]createValueGroup)
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i)
		t, ok := valueTime(elem, db.Statement.Schema, cfg.ShardingKey)
		if !ok {
			return nil, fmt.Errorf("gorm_sharding: sharding key %s must be time.Time", cfg.ShardingKey)
		}
		table := cfg.tableName(t)
		group, ok := groups[table]
		if !ok {
			group.values = reflect.MakeSlice(groupSliceType(value), 0, 1)
			group.original = value
		}
		// 同一批数据按目标分表聚合，后续每个分表执行一次批量插入。
		group.values = reflect.Append(group.values, elem)
		group.sourceIndexes = append(group.sourceIndexes, i)
		groups[table] = group
	}
	return groups, nil
}

// copyGeneratedValues 把 GORM 在分表插入中回填的字段同步给调用方的 []T。
// []*T 的元素本身就是原对象指针，GORM 已直接完成回填，无需再次复制。
func (group createValueGroup) copyGeneratedValues() {
	if (group.original.Kind() != reflect.Slice && group.original.Kind() != reflect.Array) || group.original.Type().Elem().Kind() == reflect.Ptr {
		return
	}

	for groupIndex, sourceIndex := range group.sourceIndexes {
		group.original.Index(sourceIndex).Set(group.values.Index(groupIndex))
	}
}

// groupSliceType 返回用于分组的切片类型。GORM 支持数组批量操作，但 reflect.MakeSlice
// 只能创建切片，因此数组分组需要转换为同元素类型的临时切片。
func groupSliceType(value reflect.Value) reflect.Type {
	if value.Kind() == reflect.Array {
		return reflect.SliceOf(value.Type().Elem())
	}
	return value.Type()
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
