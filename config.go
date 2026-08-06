package gorm_sharding

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ShardingStrategy 定义时间分表策略需要提供的能力。
type ShardingStrategy interface {
	// Suffix 根据分表时间计算表名后缀。
	Suffix(time.Time) string
	// Prev 返回当前时间所属分片的上一个分片时间，用于最近 N 表倒推扫描。
	Prev(time.Time) time.Time
}

// ShardingConfig 定义单个模型的分表配置。
type ShardingConfig struct {
	// ShardingKey 是分表字段的数据库列名，例如 created_at。
	ShardingKey string

	// Strategy 是分表策略，决定表名后缀和倒推扫描粒度。
	Strategy ShardingStrategy

	// MaxScanTables 是无分表条件时最多扫描的最近分表数量。
	MaxScanTables int

	// AutoCreateTable 控制插入目标分表不存在时是否自动建表。
	AutoCreateTable bool

	// AutoMigrate 控制调用插件 AutoMigrate 时是否迁移该模型的历史分表。
	AutoMigrate bool

	// tablePrefix 是从模型逻辑表名解析出来的真实分表前缀，例如 user 会生成 user_202608。
	tablePrefix string
}

// validate 校验分表配置是否具备运行所需的必要参数。
func (c ShardingConfig) validate() error {
	if c.ShardingKey == "" {
		return fmt.Errorf("gorm_sharding: sharding key is empty")
	}
	if c.Strategy == nil {
		return fmt.Errorf("gorm_sharding: sharding strategy is nil")
	}
	if c.MaxScanTables <= 0 {
		return fmt.Errorf("gorm_sharding: max scan tables must be greater than zero")
	}
	return nil
}

// tableName 根据配置和时间生成真实分表名。
func (c ShardingConfig) tableName(t time.Time) string {
	return c.tablePrefix + "_" + c.Strategy.Suffix(t)
}

// modelKey 把 struct、指针、slice 等模型输入统一归一成 struct 类型。
func modelKey(v interface{}) reflect.Type {
	t := reflect.TypeOf(v)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t != nil && t.Kind() == reflect.Slice {
		t = t.Elem()
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	return t
}

// normalizeColumnName 去掉列名两侧空白和反引号，便于比较 GORM 条件里的字段名。
func normalizeColumnName(s string) string {
	s = strings.Trim(strings.TrimSpace(s), "`")
	if index := strings.LastIndex(s, "."); index >= 0 {
		s = s[index+1:]
	}
	return strings.Trim(s, "`")
}
