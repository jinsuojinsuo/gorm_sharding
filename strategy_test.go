package gorm_sharding

import (
	"testing"
	"time"
)

// customTestStrategy 模拟业务侧定义的可解析按日分表策略。
type customTestStrategy struct{}

// Suffix 返回测试用自定义分表后缀。
func (customTestStrategy) Suffix(t time.Time) string {
	return "custom_" + t.Format("20060102")
}

// Prev 返回测试用自定义策略的前一时间。
func (customTestStrategy) Prev(t time.Time) time.Time {
	return t.AddDate(0, 0, -1)
}

// ParseSuffix 解析测试用自定义分表后缀的周期开始时间。
func (customTestStrategy) ParseSuffix(suffix string, location *time.Location) (time.Time, bool) {
	const prefix = "custom_"
	if len(suffix) < len(prefix) || suffix[:len(prefix)] != prefix {
		return time.Time{}, false
	}
	return parseTimeSuffix("20060102", suffix[len(prefix):], location)
}

// TestMonthStrategyPrevFromMonthEnd 验证月末向前路由时一定进入上一个自然月。
func TestMonthStrategyPrevFromMonthEnd(t *testing.T) {
	tests := []struct {
		name string
		at   time.Time
		want string
	}{
		{
			name: "march 29 enters february",
			at:   time.Date(2024, time.March, 29, 12, 0, 0, 0, time.Local),
			want: "202402",
		},
		{
			name: "march 31 enters february",
			at:   time.Date(2026, time.March, 31, 12, 0, 0, 0, time.Local),
			want: "202602",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MonthStrategy.Prev(tt.at)
			if got.Format("200601") != tt.want {
				t.Fatalf("Prev(%s) = %s, want month %s", tt.at, got, tt.want)
			}
		})
	}
}

// TestAutoCleanupSupportsCustomStrategy 验证实现 ParseSuffix 的自定义策略可以启用自动清理。
func TestAutoCleanupSupportsCustomStrategy(t *testing.T) {
	err := (ShardingConfig{
		TablePrefix:     "user",
		ShardingKey:     "created_at",
		Strategy:        customTestStrategy{},
		Location:        time.Local,
		MaxScanTables:   1,
		MaxRetainTables: 1,
	}).validate()
	if err != nil {
		t.Fatalf("custom strategy with automatic cleanup returned an error: %v", err)
	}
}

// TestAutoCleanupRequiresRetentionWindow 验证自动保留周期不能小于扫描周期。
func TestAutoCleanupRequiresRetentionWindow(t *testing.T) {
	err := (ShardingConfig{
		TablePrefix:     "user",
		ShardingKey:     "created_at",
		Strategy:        DayStrategy,
		Location:        time.Local,
		MaxScanTables:   3,
		MaxRetainTables: 2,
	}).validate()
	if err == nil {
		t.Fatal("retention window smaller than scan window did not return an error")
	}
}

// TestShardingLocationIsRequired 验证分表配置必须显式指定固定时区。
func TestShardingLocationIsRequired(t *testing.T) {
	err := (ShardingConfig{
		TablePrefix:   "user",
		ShardingKey:   "created_at",
		Strategy:      DayStrategy,
		MaxScanTables: 1,
	}).validate()
	if err == nil {
		t.Fatal("missing sharding location did not return an error")
	}
}

// TestShardingLocationControlsTableName 验证表名始终使用配置时区，而不是 time.Time 自身的时区。
func TestShardingLocationControlsTableName(t *testing.T) {
	cfg := ShardingConfig{
		TablePrefix: "user",
		Strategy:    DayStrategy,
		Location:    time.FixedZone("UTC+8", 8*60*60),
	}
	at := time.Date(2026, 8, 4, 17, 0, 0, 0, time.UTC)
	if got, want := cfg.tableName(at), "user_20260805"; got != want {
		t.Fatalf("table name = %s, want %s", got, want)
	}
}
