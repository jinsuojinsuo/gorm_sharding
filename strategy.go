package gorm_sharding

import (
	"fmt"
	"time"
)

// strategyFunc 用函数组合实现 ShardingStrategy，避免为每种时间粒度写重复结构体。
type strategyFunc struct {
	suffix func(time.Time) string
	prev   func(time.Time) time.Time
}

// Suffix 调用具体策略的后缀生成函数。
func (s strategyFunc) Suffix(t time.Time) string {
	return s.suffix(t)
}

// Prev 调用具体策略的上一个分片时间计算函数。
func (s strategyFunc) Prev(t time.Time) time.Time {
	return s.prev(t)
}

var (
	// YearStrategy 按年分表，表名后缀格式为 2006。
	YearStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("2006") },
		prev:   func(t time.Time) time.Time { return t.AddDate(-1, 0, 0) },
	}
	// MonthStrategy 按月分表，表名后缀格式为 200601。
	MonthStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("200601") },
		// 先归一到当月第一天再减一个月，避免 3 月 29 日减一个月被 Go 规范化为 3 月 1 日。
		prev: func(t time.Time) time.Time {
			return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).AddDate(0, -1, 0)
		},
	}
	// WeekStrategy 按 ISO 周分表，表名后缀格式为 2006_w01。
	WeekStrategy = strategyFunc{
		suffix: func(t time.Time) string {
			year, week := t.ISOWeek()
			return fmt.Sprintf("%04d_w%02d", year, week)
		},
		prev: func(t time.Time) time.Time { return t.AddDate(0, 0, -7) },
	}
	// DayStrategy 按日分表，表名后缀格式为 20060102。
	DayStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("20060102") },
		prev:   func(t time.Time) time.Time { return t.AddDate(0, 0, -1) },
	}
	// HourStrategy 按小时分表，表名后缀格式为 2006010215。
	HourStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("2006010215") },
		prev:   func(t time.Time) time.Time { return t.Add(-time.Hour) },
	}
)
