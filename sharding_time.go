package gorm_sharding

import (
	"fmt"
	"strings"
	"time"
)

// parseShardingTime 解析分表字段时间，并统一转换到配置的固定时区。
// time.Time 和带时区 RFC3339 字符串保留原瞬间后转换；无时区字符串按 Location 解释。
func parseShardingTime(value interface{}, location *time.Location) (time.Time, error) {
	if location == nil {
		return time.Time{}, fmt.Errorf("gorm_sharding: sharding location is nil")
	}
	if at, ok := value.(time.Time); ok {
		if at.IsZero() {
			return time.Time{}, fmt.Errorf("gorm_sharding: sharding time is zero")
		}
		return at.In(location), nil
	}
	text, ok := value.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("gorm_sharding: sharding time must be time.Time or supported string")
	}
	if at, err := time.Parse(time.RFC3339, text); err == nil {
		return at.In(location), nil
	}
	for _, layout := range []string{"2006-01-02", "2006/01/02", "2006-01-02 15:04:05", "2006/01/02 15:04:05", "2006-01-02T15:04:05"} {
		if at, err := time.ParseInLocation(layout, text, location); err == nil {
			return at, nil
		}
	}
	return time.Time{}, fmt.Errorf("gorm_sharding: unsupported sharding time string %q", text)
}

// parseShardingLikePrefix 将连续日期 LIKE 前缀转换为 [start, end) 时间范围。
func parseShardingLikePrefix(pattern string, location *time.Location) (time.Time, time.Time, error) {
	location = effectiveShardingLocation(location)
	if strings.Count(pattern, "%") != 1 || !strings.HasSuffix(pattern, "%") || strings.Contains(pattern, "_") {
		return time.Time{}, time.Time{}, fmt.Errorf("gorm_sharding: unsupported sharding time LIKE pattern %q", pattern)
	}
	prefix := strings.TrimSuffix(pattern, "%")
	layouts := []struct {
		layout string
		next   func(time.Time) time.Time
	}{
		{"2006", func(t time.Time) time.Time { return t.AddDate(1, 0, 0) }},
		{"2006-01", func(t time.Time) time.Time { return t.AddDate(0, 1, 0) }},
		{"2006-01-02", func(t time.Time) time.Time { return t.AddDate(0, 0, 1) }},
		{"2006-01-02 15", func(t time.Time) time.Time { return t.Add(time.Hour) }},
	}
	for _, candidate := range layouts {
		if at, err := time.ParseInLocation(candidate.layout, prefix, location); err == nil {
			return at, candidate.next(at), nil
		}
	}
	return time.Time{}, time.Time{}, fmt.Errorf("gorm_sharding: unsupported sharding time LIKE pattern %q", pattern)
}

// normalizeShardingTime 将可识别的分表时间统一转换为固定时区。
func normalizeShardingTime(value interface{}, location *time.Location) (interface{}, error) {
	at, err := parseShardingTime(value, location)
	if err != nil {
		return nil, err
	}
	return at, nil
}
