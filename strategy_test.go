package gorm_sharding

import (
	"testing"
	"time"
)

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
