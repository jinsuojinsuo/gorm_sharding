package gorm_sharding

import (
	"fmt"
	"strconv"
	"time"
)

// strategyFunc 用函数组合实现 ShardingStrategy，避免为每种时间粒度写重复结构体。
type strategyFunc struct {
	suffix func(time.Time) string
	prev   func(time.Time) time.Time
	parse  func(string, *time.Location) (time.Time, bool)
}

// Suffix 调用具体策略的后缀生成函数。
func (s strategyFunc) Suffix(t time.Time) string {
	return s.suffix(t)
}

// Prev 调用具体策略的上一个分片时间计算函数。
func (s strategyFunc) Prev(t time.Time) time.Time {
	return s.prev(t)
}

// ParseSuffix 将不包含表前缀的真实分表后缀解析为指定时区下的周期开始时间。
// 例如 MonthStrategy 的 "202608" 会解析为 2026-08-01 00:00:00。
func (s strategyFunc) ParseSuffix(suffix string, location *time.Location) (time.Time, bool) {
	return s.parse(suffix, location)
}

var (
	// YearStrategy 按年分表，表名后缀格式为 2006。
	YearStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("2006") },
		prev:   func(t time.Time) time.Time { return t.AddDate(-1, 0, 0) },
		parse: func(suffix string, location *time.Location) (time.Time, bool) {
			return parseTimeSuffix("2006", suffix, location)
		},
	}
	// MonthStrategy 按月分表，表名后缀格式为 200601。
	MonthStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("200601") },
		// 先归一到当月第一天再减一个月，避免 3 月 29 日减一个月被 Go 规范化为 3 月 1 日。
		prev: func(t time.Time) time.Time {
			return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).AddDate(0, -1, 0)
		},
		parse: func(suffix string, location *time.Location) (time.Time, bool) {
			return parseTimeSuffix("200601", suffix, location)
		},
	}
	// WeekStrategy 按 ISO 周分表，表名后缀格式为 2006_w01。
	WeekStrategy = strategyFunc{
		suffix: func(t time.Time) string {
			year, week := t.ISOWeek()
			return fmt.Sprintf("%04d_w%02d", year, week)
		},
		prev:  func(t time.Time) time.Time { return t.AddDate(0, 0, -7) },
		parse: parseISOWeekSuffix,
	}
	// DayStrategy 按日分表，表名后缀格式为 20060102。
	DayStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("20060102") },
		prev:   func(t time.Time) time.Time { return t.AddDate(0, 0, -1) },
		parse: func(suffix string, location *time.Location) (time.Time, bool) {
			return parseTimeSuffix("20060102", suffix, location)
		},
	}
	// HourStrategy 按小时分表，表名后缀格式为 2006010215。
	HourStrategy = strategyFunc{
		suffix: func(t time.Time) string { return t.Format("2006010215") },
		prev:   func(t time.Time) time.Time { return t.Add(-time.Hour) },
		parse: func(suffix string, location *time.Location) (time.Time, bool) {
			return parseTimeSuffix("2006010215", suffix, location)
		},
	}
)

// parseTimeSuffix 使用固定时区解析格式固定的分表后缀。
func parseTimeSuffix(layout, suffix string, location *time.Location) (time.Time, bool) {
	if location == nil {
		return time.Time{}, false
	}
	parsed, err := time.ParseInLocation(layout, suffix, location)
	if err != nil || parsed.Format(layout) != suffix {
		return time.Time{}, false
	}
	return parsed, true
}

// parseISOWeekSuffix 将 2006_w01 格式后缀解析为该 ISO 周周一的零点。
func parseISOWeekSuffix(suffix string, location *time.Location) (time.Time, bool) {
	if location == nil || len(suffix) != 8 || suffix[4:6] != "_w" {
		return time.Time{}, false
	}
	year, yearErr := strconv.Atoi(suffix[:4])
	week, weekErr := strconv.Atoi(suffix[6:])
	if yearErr != nil || weekErr != nil || week < 1 {
		return time.Time{}, false
	}

	janFourth := time.Date(year, time.January, 4, 0, 0, 0, 0, location)
	weekdayOffset := (int(janFourth.Weekday()) + 6) % 7
	start := janFourth.AddDate(0, 0, -weekdayOffset+7*(week-1))
	parsedYear, parsedWeek := start.ISOWeek()
	if parsedYear != year || parsedWeek != week {
		return time.Time{}, false
	}
	return start, true
}
