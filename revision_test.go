package gorm_sharding

import (
	"strings"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"vitess.io/vitess/go/vt/sqlparser"
)

// revisionDistinctUser 是跨分表 DISTINCT 验收测试使用的模型。
type revisionDistinctUser struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name;type:varchar(64);not null;default:'';index"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime;not null;index"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime;not null"`
}

// TableName 返回 DISTINCT 验收测试的独立逻辑表名，避免清理其他测试表。
func (revisionDistinctUser) TableName() string {
	return "gs_req_revision_distinct_user"
}

// TestRegisterRejectsAfterInitialize 防止初始化后再注册模型导致未解析表前缀或并发读写配置。
func TestRegisterRejectsAfterInitialize(t *testing.T) {
	plugin := New()
	plugin.initialized = true

	if err := plugin.Register(requirementUser{}, ShardingConfig{
		ShardingKey:   "created_at",
		Strategy:      DayStrategy,
		MaxScanTables: 1,
	}); err == nil {
		t.Fatal("Register after initialization returned nil error")
	}
}

// TestTableNameLikePatternEscapesWildcards 防止逻辑表名前缀中的 LIKE 通配符匹配到其他业务表。
func TestTableNameLikePatternEscapesWildcards(t *testing.T) {
	if got, want := tableNameLikePattern("user_log%v"), "user\\_log\\%v\\_%"; got != want {
		t.Fatalf("LIKE pattern = %q, want %q", got, want)
	}
}

// TestRouteDoesNotMatchSimilarColumnName 防止 other_created_at 等相似字段被误识别为分表字段。
func TestRouteDoesNotMatchSimilarColumnName(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 1}
	at := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)

	if _, ok := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "other_created_at = ?", Vars: []interface{}{at}},
	}, cfg, "created_at"); ok {
		t.Fatal("similar column name was treated as the sharding key")
	}
}

// TestHalfOpenRangeWithOneShardScansStartShard 防止 [start, end) 的上界把唯一扫描槽位占掉。
func TestHalfOpenRangeWithOneShardScansStartShard(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: HourStrategy, MaxScanTables: 1}
	start := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	end := start.Add(time.Hour)

	tables, ok := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "created_at >= ? AND created_at < ?", Vars: []interface{}{start, end}},
	}, cfg, "created_at")
	if !ok {
		t.Fatal("half-open range was not recognized")
	}
	if want := []string{"user_2026080410"}; !sameStrings(tables, want) {
		t.Fatalf("tables = %v, want %v", tables, want)
	}
}

// TestRawStatementRoutesHistoricalExactTime 防止 Raw 查询忽略时间条件而固定路由到最新分表。
func TestRawStatementRoutesHistoricalExactTime(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 2}
	oldAt := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	stmt, err := sqlparser.NewTestParser().Parse("SELECT * FROM user WHERE created_at = ?")
	if err != nil {
		t.Fatalf("parse raw SQL: %v", err)
	}

	tables, routed := rawStatementTables(stmt, []interface{}{oldAt}, cfg, "created_at")
	if !routed || !sameStrings(tables, []string{"user_20260802"}) {
		t.Fatalf("tables = %v, routed = %v", tables, routed)
	}
}

// TestRawStatementDetectsMultipleShards 防止 Raw 跨分表条件被静默改写到一张表。
func TestRawStatementDetectsMultipleShards(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 2}
	t1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	t2 := t1.AddDate(0, 0, 1)
	stmt, err := sqlparser.NewTestParser().Parse("SELECT * FROM user WHERE created_at IN (?, ?)")
	if err != nil {
		t.Fatalf("parse raw SQL: %v", err)
	}

	tables, routed := rawStatementTables(stmt, []interface{}{t1, t2}, cfg, "created_at")
	if !routed || !sameStrings(tables, []string{"user_20260802", "user_20260803"}) {
		t.Fatalf("tables = %v, routed = %v", tables, routed)
	}
}

// TestRouteParsesSliceArgumentIn 防止 created_at IN ? 的时间切片退化为最近表扫描。
func TestRouteParsesSliceArgumentIn(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 2}
	t1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	t2 := t1.AddDate(0, 0, 1)
	tables, ok := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "created_at IN ?", Vars: []interface{}{[]time.Time{t1, t2}}},
	}, cfg, "created_at")
	if !ok || !sameStrings(tables, []string{"user_20260802", "user_20260803"}) {
		t.Fatalf("tables = %v, routed = %v", tables, ok)
	}
}

// TestCombinedQueryBuildsGlobalOrderAndLimit 验证跨分表排序和分页使用 MySQL 外层统一处理。
func TestCombinedQueryBuildsGlobalOrderAndLimit(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	plugin := New()
	plugin.queryFn = callbacks.Query
	start := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	query := db.Model(&requirementUser{}).
		Where("created_at BETWEEN ? AND ?", start, start.AddDate(0, 0, 1)).
		Order("score DESC").
		Offset(1).
		Limit(2)

	sql, vars, err := plugin.buildCombinedQuery(query, []string{"gs_req_user_20260802", "gs_req_user_20260803"})
	if err != nil {
		t.Fatalf("build combined query: %v", err)
	}
	if !strings.Contains(sql, "union all") {
		t.Fatalf("combined SQL does not contain UNION ALL: %s", sql)
	}
	if !strings.Contains(sql, "order by score desc limit 1, 2") {
		t.Fatalf("combined SQL does not contain outer order and limit: %s", sql)
	}
	if len(vars) != 4 || vars[0] != start || vars[2] != start {
		t.Fatalf("combined vars = %#v, want duplicated range variables", vars)
	}
}

// TestCombinedQueryBuildsGlobalGroupBy 验证跨分表分组在 UNION ALL 外层完成聚合。
func TestCombinedQueryBuildsGlobalGroupBy(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	plugin := New()
	plugin.queryFn = callbacks.Query
	start := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	query := db.Model(&requirementUser{}).
		Select("name, COUNT(*) AS total").
		Where("created_at BETWEEN ? AND ?", start, start.AddDate(0, 0, 1)).
		Group("name")

	sql, _, err := plugin.buildCombinedQuery(query, []string{"gs_req_user_20260802", "gs_req_user_20260803"})
	if err != nil {
		t.Fatalf("build combined query: %v", err)
	}
	if !strings.Contains(sql, "union all") || !strings.Contains(sql, "count(*) as total") || !strings.Contains(sql, "group by") {
		t.Fatalf("combined group SQL = %s", sql)
	}
}

// TestAggregateQueryDetection 验证 Count 和常用聚合选择表达式会进入跨分表聚合路径。
func TestAggregateQueryDetection(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	countQuery := db.Model(&requirementUser{})
	countQuery.Statement.AddClause(clause.Select{Expression: clause.Expr{SQL: "count(*)"}})
	if !isAggregateQuery(countQuery) {
		t.Fatal("count query was not detected as aggregate")
	}
	if !isAggregateQuery(db.Model(&requirementUser{}).Select("SUM(score), MIN(score), MAX(score), AVG(score)")) {
		t.Fatal("common aggregate select was not detected")
	}
}

// TestRevisionCrossShardCountDistinct 验证 COUNT(DISTINCT ...) 对所有命中分表统一去重。
func TestRevisionCrossShardCountDistinct(t *testing.T) {
	prefix := revisionDistinctUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, revisionDistinctUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	users := []revisionDistinctUser{
		{Name: "alice", CreatedAt: day1, UpdatedAt: day1},
		{Name: "alice", CreatedAt: day2, UpdatedAt: day2},
		{Name: "bob", CreatedAt: day2, UpdatedAt: day2},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("create distinct users: %v", err)
	}

	var count int64
	result := db.Model(&revisionDistinctUser{}).
		Distinct("name").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Count(&count)
	if result.Error != nil {
		t.Fatalf("cross-shard count distinct: %v", result.Error)
	}
	if count != 2 {
		t.Fatalf("cross-shard distinct count = %d, want 2", count)
	}
}
