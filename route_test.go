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
		MaxScanTables: 10,
		tablePrefix:   "user",
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
			want:  []string{"user_20260804"},
		},
		{
			name:  "clause range",
			exprs: []clause.Expression{clause.Gte{Column: "created_at", Value: t1}, clause.Lt{Column: "created_at", Value: t2}},
			want:  []string{"user_20260804"},
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
			got, ok := tablesFromExprs(tt.exprs, cfg, "created_at")
			if !ok {
				t.Fatalf("where expression was not recognized")
			}
			if !sameStrings(got, tt.want) {
				t.Fatalf("tables = %v, want %v", got, tt.want)
			}
		})
	}
}
