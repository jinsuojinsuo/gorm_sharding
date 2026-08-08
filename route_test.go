package gorm_sharding

import (
	"testing"
	"time"

	"gorm.io/gorm/clause"
)

// TestRouteExtractsTablesFromCommonWhereForms 验证常见 GORM where 写法都能路由到明确分表。
func TestRouteExtractsTablesFromCommonWhereForms(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}
	t1 := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	t2 := time.Date(2026, 8, 5, 10, 0, 0, 0, time.Local)

	cases := []struct {
		name  string
		exprs []clause.Expression
		want  []string
	}{
		{
			name:  "compound equality",
			exprs: []clause.Expression{clause.Expr{SQL: "created_at = ? AND name = ?", Vars: []interface{}{t1, "alice"}}},
			want:  []string{"user_20260804"},
		},
		{
			name:  "qualified equality",
			exprs: []clause.Expression{clause.Expr{SQL: "users.created_at = ? AND name = ?", Vars: []interface{}{t1, "alice"}}},
			want:  []string{"user_20260804"},
		},
		{
			name:  "quoted qualified equality",
			exprs: []clause.Expression{clause.Expr{SQL: "`users`.`created_at` = ? AND name = ?", Vars: []interface{}{t1, "alice"}}},
			want:  []string{"user_20260804"},
		},
		{
			name:  "half open range",
			exprs: []clause.Expression{clause.Expr{SQL: "created_at >= ? AND created_at < ?", Vars: []interface{}{t1, t2}}},
			want:  []string{"user_20260805", "user_20260804"},
		},
		{
			name:  "clause range",
			exprs: []clause.Expression{clause.Gte{Column: "created_at", Value: t1}, clause.Lt{Column: "created_at", Value: t2}},
			want:  []string{"user_20260805", "user_20260804"},
		},
		{
			name:  "clause in",
			exprs: []clause.Expression{clause.IN{Column: "created_at", Values: []interface{}{t1, t2}}},
			want:  []string{"user_20260804", "user_20260805"},
		},
		{
			name:  "sql in placeholders",
			exprs: []clause.Expression{clause.Expr{SQL: "created_at IN (?, ?) AND name = ?", Vars: []interface{}{t1, t2, "alice"}}},
			want:  []string{"user_20260804", "user_20260805"},
		},
		{
			name:  "or equality",
			exprs: []clause.Expression{clause.OrConditions{Exprs: []clause.Expression{clause.Eq{Column: "created_at", Value: t1}, clause.Eq{Column: "created_at", Value: t2}}}},
			want:  []string{"user_20260804", "user_20260805"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := tablesFromExprs(tt.exprs, cfg, "created_at")
			if err != nil {
				t.Fatalf("tablesFromExprs error = %v", err)
			}
			if !ok {
				t.Fatalf("where expression was not recognized")
			}
			if !sameStrings(got, tt.want) {
				t.Fatalf("tables = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRouteLikeDoesNotMatchSimilarColumnName 防止 other_created_at LIKE ? 被误识别为分表字段 LIKE。
func TestRouteLikeDoesNotMatchSimilarColumnName(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}

	if _, ok, err := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "other_created_at LIKE ?", Vars: []interface{}{"2026%"}},
	}, cfg, "created_at"); err != nil || ok {
		t.Fatalf("similar LIKE routed = %v, err = %v; want not routed", ok, err)
	}
}

// TestRouteNormalizesOnlyShardingKeyTimeStrings 验证只有分表字段的时间字符串会按配置时区解释并回写参数。
func TestRouteNormalizesOnlyShardingKeyTimeStrings(t *testing.T) {
	location := time.FixedZone("UTC+8", 8*60*60)
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      location,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}
	expr := clause.Expr{
		SQL:  "created_at = ? AND updated_at = ?",
		Vars: []interface{}{"2026-08-04T00:00:00Z", "2026-08-04T00:00:00Z"},
	}

	got, ok, err := tablesFromExprs([]clause.Expression{expr}, cfg, "created_at")
	if err != nil {
		t.Fatalf("tablesFromExprs error = %v", err)
	}
	if !ok {
		t.Fatalf("where expression was not recognized")
	}
	if !sameStrings(got, []string{"user_20260804"}) {
		t.Fatalf("tables = %v, want user_20260804", got)
	}
	normalized, ok := expr.Vars[0].(time.Time)
	if !ok || normalized.Location() != location || normalized.Hour() != 8 {
		t.Fatalf("created_at var = %#v, want time in UTC+8 at 08:00", expr.Vars[0])
	}
	if expr.Vars[1] != "2026-08-04T00:00:00Z" {
		t.Fatalf("updated_at var changed to %#v", expr.Vars[1])
	}
}

// TestRouteRejectsUnsupportedShardingTimeInputs 验证分表字段上的非法时间不会退化为最近周期扫描。
func TestRouteRejectsUnsupportedShardingTimeInputs(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}
	cases := []struct {
		name  string
		exprs []clause.Expression
	}{
		{name: "bad string", exprs: []clause.Expression{clause.Expr{SQL: "created_at = ?", Vars: []interface{}{"2026.08.04"}}}},
		{name: "inline literal", exprs: []clause.Expression{clause.Expr{SQL: "created_at = '2026-08-04'"}}},
		{name: "bad like", exprs: []clause.Expression{clause.Expr{SQL: "created_at LIKE ?", Vars: []interface{}{"2026-0%"}}}},
		{name: "unsupported expression", exprs: []clause.Expression{clause.Expr{SQL: "DATE(created_at) = ?", Vars: []interface{}{"2026-08-04"}}}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := tablesFromExprs(tt.exprs, cfg, "created_at"); err == nil {
				t.Fatal("tablesFromExprs returned nil error")
			}
		})
	}
}
