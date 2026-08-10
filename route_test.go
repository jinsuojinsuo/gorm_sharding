package gorm_sharding

import (
	"testing"
	"time"

	"gorm.io/gorm/clause"
	"vitess.io/vitess/go/vt/sqlparser"
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

// TestRouteNormalizesAllShardingTimeConditions 验证精确路由不能跳过后续分表字段参数归一化。
func TestRouteNormalizesAllShardingTimeConditions(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}
	createdAt := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	expr := clause.Expr{
		SQL:  "created_at = ? AND created_at >= ?",
		Vars: []interface{}{createdAt, "2026/08/04"},
	}

	tables, routed, err := tablesFromExprs([]clause.Expression{expr}, cfg, "created_at")
	if err != nil {
		t.Fatalf("tablesFromExprs error = %v", err)
	}
	if !routed || !sameStrings(tables, []string{"user_20260804"}) {
		t.Fatalf("tables = %v, routed = %v", tables, routed)
	}
	if _, ok := expr.Vars[1].(time.Time); !ok {
		t.Fatalf("range var type = %T, want time.Time", expr.Vars[1])
	}
}

// TestRouteRejectsInvalidShardingTimeAfterLike 验证 LIKE 路由不能跳过后续非法分表字段条件。
func TestRouteRejectsInvalidShardingTimeAfterLike(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}

	if _, _, err := tablesFromExprs([]clause.Expression{clause.Expr{
		SQL:  "created_at LIKE ? AND created_at = ?",
		Vars: []interface{}{"2026-08%", "not-a-time"},
	}}, cfg, "created_at"); err == nil {
		t.Fatal("tablesFromExprs returned nil error")
	}
}

// TestRouteFallsBackAfterNormalizingSingleBound 验证合法但无法形成完整范围的条件扫描最近分表。
func TestRouteFallsBackAfterNormalizingSingleBound(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}
	expr := clause.Expr{SQL: "created_at >= ?", Vars: []interface{}{"2026/08/04"}}

	if _, routed, err := tablesFromExprs([]clause.Expression{expr}, cfg, "created_at"); err != nil || routed {
		t.Fatalf("tablesFromExprs routed = %v, err = %v; want fallback", routed, err)
	}
	if _, ok := expr.Vars[0].(time.Time); !ok {
		t.Fatalf("range var type = %T, want time.Time", expr.Vars[0])
	}
}

// TestRouteIntersectsLikeAndRange 验证 LIKE 前缀范围会与显式上下界按 AND 语义取交集。
func TestRouteIntersectsLikeAndRange(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}
	expr := clause.Expr{
		SQL:  "created_at >= ? AND created_at < ? AND created_at LIKE ?",
		Vars: []interface{}{"2026/08/02", "2026/08/03", "2026-08%"},
	}

	tables, routed, err := tablesFromExprs([]clause.Expression{expr}, cfg, "created_at")
	if err != nil {
		t.Fatalf("tablesFromExprs error = %v", err)
	}
	if !routed || !sameStrings(tables, []string{"user_20260802"}) {
		t.Fatalf("tables = %v, routed = %v", tables, routed)
	}
	for _, index := range []int{0, 1} {
		if _, ok := expr.Vars[index].(time.Time); !ok {
			t.Fatalf("range var %d type = %T, want time.Time", index, expr.Vars[index])
		}
	}
}

// TestRawRouteIntersectsLikeAndRange 验证 Raw SQL 与 GORM 条件使用相同的范围交集路由。
func TestRawRouteIntersectsLikeAndRange(t *testing.T) {
	cfg := ShardingConfig{
		Strategy:      DayStrategy,
		Location:      time.Local,
		MaxScanTables: 10,
		TablePrefix:   "user",
	}
	stmt, err := sqlparser.NewTestParser().Parse(
		"SELECT * FROM user WHERE created_at >= ? AND created_at < ? AND created_at LIKE ?",
	)
	if err != nil {
		t.Fatalf("parse raw SQL: %v", err)
	}
	vars := []interface{}{"2026/08/02", "2026/08/03", "2026-08%"}

	tables, routed, err := rawStatementTables(stmt, vars, cfg, "created_at")
	if err != nil {
		t.Fatalf("rawStatementTables error = %v", err)
	}
	if !routed || !sameStrings(tables, []string{"user_20260802"}) {
		t.Fatalf("tables = %v, routed = %v", tables, routed)
	}
	for _, index := range []int{0, 1} {
		if _, ok := vars[index].(time.Time); !ok {
			t.Fatalf("range var %d type = %T, want time.Time", index, vars[index])
		}
	}
}
