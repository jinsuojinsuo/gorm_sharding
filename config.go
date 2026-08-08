package gorm_sharding

import (
	"fmt"
	"strings"
	"time"
)

// ShardingStrategy 定义时间分表策略需要提供的能力。
type ShardingStrategy interface {
	// Suffix 根据分表时间计算表名后缀。
	Suffix(time.Time) string
	// Prev 返回当前时间所属分片的上一个分片时间，用于最近 N 个周期倒推扫描。
	Prev(time.Time) time.Time
	// ParseSuffix 将不包含 TablePrefix 的真实分表后缀解析为所属周期的开始时间。
	// 返回时间必须使用传入 location，例如按月后缀 "202608" 应返回
	// 2026-08-01 00:00:00，按日后缀 "20260804" 应返回 2026-08-04 00:00:00。
	// 后缀不属于当前策略、格式或日期无效时返回 false；自动清理会保留无法解析的同前缀表。
	ParseSuffix(suffix string, location *time.Location) (time.Time, bool)
}

// ShardingConfig 定义单个模型的分表配置。
type ShardingConfig struct {
	// TablePrefix 是逻辑表名和真实分表前缀，例如 users 会生成 users_202608。
	// 必须与业务模型的 TableName() 返回值（或 GORM 默认表名）保持一致。
	TablePrefix string

	// ShardingKey 是分表字段的数据库列名，例如 created_at。
	ShardingKey string

	// Strategy 是分表策略，决定表名后缀和倒推扫描粒度。
	Strategy ShardingStrategy

	// Location 是分表使用的固定时区，例如 time.Local 或 time.UTC。
	// 必须显式配置，所有表名计算和周期倒推都会先转换到该时区。
	Location *time.Location

	// MaxScanTables 是无分表条件时最多扫描的最近连续时间周期数。
	MaxScanTables int

	// MaxRetainTables 是自动保留的连续时间周期数；0 表示不自动删除历史分表。
	// 大于 0 时 Strategy 必须正确实现 ParseSuffix。
	MaxRetainTables int

	// AutoCreateTable 控制插入目标分表不存在时是否自动建表。
	AutoCreateTable bool

	// AutoMigrate 控制调用插件 AutoMigrate 时是否迁移该模型的历史分表。
	AutoMigrate bool
}

// validate 校验分表配置是否具备运行所需的必要参数。
func (c ShardingConfig) validate() error {
	if c.TablePrefix == "" {
		return fmt.Errorf("gorm_sharding: table prefix is empty")
	}
	if c.ShardingKey == "" {
		return fmt.Errorf("gorm_sharding: sharding key is empty")
	}
	if c.Strategy == nil {
		return fmt.Errorf("gorm_sharding: sharding strategy is nil")
	}
	if c.Location == nil {
		return fmt.Errorf("gorm_sharding: sharding location is nil")
	}
	if c.MaxScanTables <= 0 {
		return fmt.Errorf("gorm_sharding: max scan tables must be greater than zero")
	}
	if c.MaxRetainTables < 0 {
		return fmt.Errorf("gorm_sharding: max retain tables must not be negative")
	}
	if c.MaxRetainTables > 0 && c.MaxRetainTables < c.MaxScanTables {
		return fmt.Errorf("gorm_sharding: max retain tables must be greater than or equal to max scan tables")
	}
	return nil
}

// tableName 根据配置和时间生成真实分表名。
func (c ShardingConfig) tableName(t time.Time) string {
	return c.TablePrefix + "_" + c.Strategy.Suffix(c.shardTime(t))
}

// shardTime 将时间转换为配置的固定分表时区。
// tableName 等内部辅助函数也会被未注册的测试配置调用，空时区时保留原时间；真实插件配置会在 Register 时拒绝空时区。
func (c ShardingConfig) shardTime(t time.Time) time.Time {
	if c.Location == nil {
		return t
	}
	return t.In(c.Location)
}

// normalizeColumnName 去掉列名两侧空白和反引号，便于比较 GORM 条件里的字段名。
func normalizeColumnName(s string) string {
	s = strings.Trim(strings.TrimSpace(s), "`")
	if index := strings.LastIndex(s, "."); index >= 0 {
		s = s[index+1:]
	}
	return strings.Trim(s, "`")
}
