package gorm_sharding

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
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

// revisionDuplicateUser 和 revisionDuplicateOrder 故意使用同一逻辑表名，用于初始化校验测试。
type revisionDuplicateUser struct {
	ID        uint64
	CreatedAt time.Time
}

// TableName 返回重复测试使用的逻辑表名。
func (revisionDuplicateUser) TableName() string { return "gs_req_revision_duplicate" }

// revisionDuplicateOrder 是与 revisionDuplicateUser 不同的模型类型。
type revisionDuplicateOrder struct {
	ID        uint64
	CreatedAt time.Time
}

// TableName 返回与 revisionDuplicateUser 相同的逻辑表名。
func (revisionDuplicateOrder) TableName() string { return "gs_req_revision_duplicate" }

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

// TestRangeRouteUsesIntersectionBounds 验证多个范围条件按交集计算，避免条件顺序导致漏扫。
func TestRangeRouteUsesIntersectionBounds(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 10}
	day1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	day3 := day1.AddDate(0, 0, 2)
	day4 := day1.AddDate(0, 0, 3)

	tables, ok := tablesFromExprs([]clause.Expression{clause.Expr{
		SQL:  "created_at >= ? AND created_at >= ? AND created_at < ? AND created_at < ?",
		Vars: []interface{}{day1, day2, day4, day3},
	}}, cfg, "created_at")
	if !ok {
		t.Fatal("range intersection was not recognized")
	}
	if want := []string{"user_20260802"}; !sameStrings(tables, want) {
		t.Fatalf("tables = %v, want %v", tables, want)
	}
}

// TestRangeRouteMergesSeparateWhereExpressions 验证多次 Where 产生的独立表达式会合并为同一范围。
func TestRangeRouteMergesSeparateWhereExpressions(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 10}
	start := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 0, 2)

	tables, ok := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "created_at >= ?", Vars: []interface{}{start}},
		clause.Expr{SQL: "created_at < ?", Vars: []interface{}{end}},
	}, cfg, "created_at")
	if !ok {
		t.Fatal("separate range expressions were not recognized")
	}
	if want := []string{"user_20260803", "user_20260802"}; !sameStrings(tables, want) {
		t.Fatalf("tables = %v, want %v", tables, want)
	}
}

// TestRangeRouteMergesChainedWhere 验证 GORM 连续 Where 产生的 Statement 也能精确路由。
func TestRangeRouteMergesChainedWhere(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 10}
	start := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 0, 2)
	query := db.Model(&requirementUser{}).
		Where("created_at >= ?", start).
		Where("created_at < ?", end)
	where, ok := query.Statement.Clauses["WHERE"].Expression.(clause.Where)
	if !ok {
		t.Fatal("GORM did not create WHERE clause")
	}
	tables, ok := tablesFromExprs(where.Exprs, cfg, "created_at")
	if !ok {
		t.Fatal("chained Where range was not recognized")
	}
	if want := []string{"user_20260803", "user_20260802"}; !sameStrings(tables, want) {
		t.Fatalf("tables = %v, want %v", tables, want)
	}
}

// TestReadRouteIgnoresResultValue 验证 Find 结果对象已有分表时间时，读取仍只按 WHERE 路由。
func TestReadRouteIgnoresResultValue(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}

	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 10}
	start := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 0, 2)
	result := revisionDistinctUser{CreatedAt: start.AddDate(0, 0, 30)}
	query := db.Model(&revisionDistinctUser{}).
		Where("created_at >= ?", start).
		Where("created_at < ?", end)
	// GORM 在 Find(&result) 时会把此对象放进 ReflectValue。这里显式设置它，
	// 以验证读取路由不会再将结果对象的 CreatedAt 当作查询条件。
	query.Statement.ReflectValue = reflect.ValueOf(&result)

	tables := New().routeReadTables(query, cfg)
	if want := []string{"user_20260803", "user_20260802"}; !sameStrings(tables, want) {
		t.Fatalf("tables = %v, want %v", tables, want)
	}
}

// TestReverseRangeRoutesNoTables 验证上界早于下界的矛盾范围不扫描任何分表。
func TestReverseRangeRoutesNoTables(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 10}
	start := time.Date(2026, 8, 4, 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 0, -2)
	tables, ok := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "created_at >= ? AND created_at < ?", Vars: []interface{}{start, end}},
	}, cfg, "created_at")
	if !ok {
		t.Fatal("reverse range was not recognized")
	}
	if len(tables) != 0 {
		t.Fatalf("tables = %v, want no tables", tables)
	}
}

// TestEqualHalfOpenRangeRoutesNoTables 验证分片边界上的 [t, t) 半开区间不会扫描历史分表。
func TestEqualHalfOpenRangeRoutesNoTables(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 10}
	at := time.Date(2026, 8, 4, 0, 0, 0, 0, time.Local)

	tables, ok := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "created_at >= ? AND created_at < ?", Vars: []interface{}{at, at}},
	}, cfg, "created_at")
	if !ok {
		t.Fatal("equal half-open range was not recognized")
	}
	if len(tables) != 0 {
		t.Fatalf("tables = %v, want no tables", tables)
	}
}

// TestEqualOpenStartRangeRoutesNoTables 验证 (t, t] 的排他下界范围不会扫描任何分表。
func TestEqualOpenStartRangeRoutesNoTables(t *testing.T) {
	cfg := ShardingConfig{tablePrefix: "user", Strategy: DayStrategy, MaxScanTables: 10}
	at := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)

	tables, ok := tablesFromExprs([]clause.Expression{
		clause.Expr{SQL: "created_at > ? AND created_at <= ?", Vars: []interface{}{at, at}},
	}, cfg, "created_at")
	if !ok {
		t.Fatal("equal open-start range was not recognized")
	}
	if len(tables) != 0 {
		t.Fatalf("tables = %v, want no tables", tables)
	}
}

// TestRawWriteLimitAcrossShardsIsRejected 验证 Raw 写入命中多张分表时拒绝 LIMIT 语义。
func TestRawWriteLimitAcrossShardsIsRejected(t *testing.T) {
	plugin := New()
	plugin.configs[reflect.TypeOf(revisionDistinctUser{})] = ShardingConfig{
		tablePrefix:   "user",
		ShardingKey:   "created_at",
		Strategy:      DayStrategy,
		MaxScanTables: 10,
	}
	start := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 0, 2)
	stmt, err := sqlparser.NewTestParser().Parse(
		"UPDATE user SET name = 'updated' WHERE created_at >= ? AND created_at < ? LIMIT 1",
	)
	if err != nil {
		t.Fatalf("parse raw update: %v", err)
	}

	_, targets, handled, err := plugin.rawWriteTargets(nil, stmt, []interface{}{start, end})
	if err != nil || !handled || len(targets) != 2 {
		t.Fatalf("targets = %v, handled = %v, err = %v", targets, handled, err)
	}
	if !rawWriteHasLimit(stmt) {
		t.Fatal("raw update LIMIT was not detected")
	}
}

// TestCrossShardLockingIsDetected 验证跨分表锁定查询会识别 GORM 的 FOR 子句。
func TestCrossShardLockingIsDetected(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	query := db.Model(&revisionDistinctUser{}).Clauses(clause.Locking{Strength: clause.LockingStrengthUpdate})
	if !hasCrossShardLocking(query) {
		t.Fatal("FOR UPDATE locking clause was not detected")
	}
}

// TestCrossShardWriteLimitIsRejected 验证普通 GORM 跨分表 Update/Delete 会拒绝 LIMIT。
func TestCrossShardWriteLimitIsRejected(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	write := db.Model(&revisionDistinctUser{}).Limit(1)
	if err := crossShardWriteLimitError(write); err == nil || err.Error() != "gorm_sharding: limit across shards is not supported" {
		t.Fatalf("cross-shard write limit error = %v", err)
	}
}

// TestGroupDeleteValuesByShard 验证批量 Delete 会按每个实体的分表时间分组。
func TestGroupDeleteValuesByShard(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	if err := db.Statement.Parse(&revisionDistinctUser{}); err != nil {
		t.Fatalf("parse delete model: %v", err)
	}
	first := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	second := first.AddDate(0, 0, 1)
	db.Statement.ReflectValue = reflect.ValueOf([]revisionDistinctUser{
		{ID: 1, CreatedAt: first},
		{ID: 2, CreatedAt: second},
	})
	cfg := ShardingConfig{tablePrefix: "user", ShardingKey: "created_at", Strategy: DayStrategy}

	groups, grouped, err := New().groupDeleteValues(db, cfg)
	if err != nil || !grouped {
		t.Fatalf("groups = %v, grouped = %v, err = %v", groups, grouped, err)
	}
	if len(groups) != 2 {
		t.Fatalf("group count = %d, want 2", len(groups))
	}
	if group := groups["user_20260802"]; group.Len() != 1 || group.Index(0).FieldByName("ID").Uint() != 1 {
		t.Fatalf("first shard group = %v", group)
	}
	if group := groups["user_20260803"]; group.Len() != 1 || group.Index(0).FieldByName("ID").Uint() != 2 {
		t.Fatalf("second shard group = %v", group)
	}
}

// TestGroupUpdateValuesByShard 验证批量 Updates 会按每个模型实体的分表时间分组。
func TestGroupUpdateValuesByShard(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	if err := db.Statement.Parse(&revisionDistinctUser{}); err != nil {
		t.Fatalf("parse update model: %v", err)
	}
	first := time.Date(2026, 8, 2, 0, 0, 0, 0, time.Local)
	second := first.AddDate(0, 0, 1)
	db.Statement.ReflectValue = reflect.ValueOf([]revisionDistinctUser{
		{ID: 1, CreatedAt: first},
		{ID: 2, CreatedAt: second},
	})
	cfg := ShardingConfig{tablePrefix: "user", ShardingKey: "created_at", Strategy: DayStrategy}

	groups, grouped, err := New().groupUpdateValues(db, cfg)
	if err != nil || !grouped {
		t.Fatalf("groups = %v, grouped = %v, err = %v", groups, grouped, err)
	}
	if len(groups) != 2 {
		t.Fatalf("group count = %d, want 2", len(groups))
	}
	if group := groups["user_20260802"]; group.Len() != 1 || group.Index(0).FieldByName("ID").Uint() != 1 {
		t.Fatalf("first shard group = %v", group)
	}
	if group := groups["user_20260803"]; group.Len() != 1 || group.Index(0).FieldByName("ID").Uint() != 2 {
		t.Fatalf("second shard group = %v", group)
	}
}

// TestBatchEntityUpdatesAcrossShards 验证批量实体 Updates 按分表分组后只使用本分表实体的主键条件。
func TestBatchEntityUpdatesAcrossShards(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	first := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	second := first.AddDate(0, 0, 1)
	users := []requirementUser{
		{Name: "first", Score: 1, CreatedAt: first, UpdatedAt: first},
		{Name: "second", Score: 2, CreatedAt: second, UpdatedAt: second},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("create batch update rows: %v", err)
	}

	result := db.Model(&users).Updates(map[string]interface{}{"score": 100})
	if result.Error != nil {
		t.Fatalf("batch entity updates: %v", result.Error)
	}
	if result.RowsAffected != 2 {
		t.Fatalf("batch entity updates RowsAffected = %d, want 2", result.RowsAffected)
	}

	var got []requirementUser
	if err := db.Where("created_at BETWEEN ? AND ?", first, second).Order("created_at ASC").Find(&got).Error; err != nil {
		t.Fatalf("find batch update rows: %v", err)
	}
	if len(got) != 2 || got[0].Score != 100 || got[1].Score != 100 {
		t.Fatalf("batch entity updated rows = %+v, want two rows with score 100", got)
	}
}

// TestReverseRangeReturnsEmptyResult 验证矛盾范围的 Find、Scan、Update 都返回空结果且不访问逻辑表。
func TestReverseRangeReturnsEmptyResult(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	if err := db.Create(&requirementUser{Name: "kept", Score: 1, CreatedAt: day, UpdatedAt: day}).Error; err != nil {
		t.Fatalf("create reverse range row: %v", err)
	}
	start := day.AddDate(0, 0, 1)
	end := day

	var users []requirementUser
	if err := db.Where("created_at >= ? AND created_at < ?", start, end).Find(&users).Error; err != nil {
		t.Fatalf("reverse range find: %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("reverse range find rows = %+v, want empty", users)
	}

	var scanned []requirementUser
	if err := db.Model(&requirementUser{}).Where("created_at >= ? AND created_at < ?", start, end).Scan(&scanned).Error; err != nil {
		t.Fatalf("reverse range scan: %v", err)
	}
	if len(scanned) != 0 {
		t.Fatalf("reverse range scan rows = %+v, want empty", scanned)
	}

	result := db.Model(&requirementUser{}).
		Where("created_at >= ? AND created_at < ?", start, end).
		Updates(map[string]interface{}{"score": 9})
	if result.Error != nil {
		t.Fatalf("reverse range update: %v", result.Error)
	}
	if result.RowsAffected != 0 {
		t.Fatalf("reverse range update RowsAffected = %d, want 0", result.RowsAffected)
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

// TestRawJoinWriteIsRejected 验证包含逻辑分表的 Raw JOIN 写入不会回退执行原始 SQL。
func TestRawJoinWriteIsRejected(t *testing.T) {
	plugin := New()
	plugin.configs[modelKey(requirementUser{})] = ShardingConfig{
		tablePrefix: "gs_req_user",
		ShardingKey: "created_at",
		Strategy:    DayStrategy,
	}
	stmt, err := sqlparser.NewTestParser().Parse(
		"UPDATE gs_req_user JOIN orders ON orders.user_id = gs_req_user.id SET gs_req_user.score = 9 WHERE gs_req_user.created_at = ?",
	)
	if err != nil {
		t.Fatalf("parse raw join update: %v", err)
	}
	_, _, handled, err := plugin.rawWriteTargets(nil, stmt, []interface{}{time.Now()})
	if !handled || err == nil {
		t.Fatalf("raw join write handled = %v, err = %v; want handled error", handled, err)
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

// TestCombinedDistinctLimitStaysInOuterQuery 验证 DISTINCT 分页不会在去重前截断分表原始行。
func TestCombinedDistinctLimitStaysInOuterQuery(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	plugin := New()
	query := db.Model(&revisionDistinctUser{}).
		Distinct("name").
		Order("name ASC").
		Limit(2)

	sql, _, err := plugin.buildCombinedQuery(query, []string{"gs_req_revision_distinct_user_20260802", "gs_req_revision_distinct_user_20260803"})
	if err != nil {
		t.Fatalf("build combined distinct query: %v", err)
	}
	if count := strings.Count(strings.ToLower(sql), "limit"); count != 1 {
		t.Fatalf("distinct query has %d LIMIT clauses, want only outer LIMIT: %s", count, sql)
	}
}

// TestCombinedAggregateLimitStaysInOuterQuery 验证聚合在完整分表原始行集上计算。
func TestCombinedAggregateLimitStaysInOuterQuery(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	plugin := New()
	query := db.Model(&requirementUser{}).
		Select("COUNT(*) AS total, SUM(score) AS score_sum, AVG(score) AS score_avg").
		Limit(1)

	sql, _, err := plugin.buildCombinedQuery(query, []string{"gs_req_user_20260802", "gs_req_user_20260803"})
	if err != nil {
		t.Fatalf("build combined aggregate query: %v", err)
	}
	if count := strings.Count(strings.ToLower(sql), "limit"); count != 1 {
		t.Fatalf("aggregate query has %d LIMIT clauses, want only outer LIMIT: %s", count, sql)
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

// TestCombinedQueryRetriesAfterMissingShard 验证组合查询在 SQL 返回 1146 后会跳过不存在的中间分表并重试一次。
func TestCombinedQueryRetriesAfterMissingShard(t *testing.T) {
	prefix := revisionDistinctUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 3, revisionDistinctUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day3 := day1.AddDate(0, 0, 2)
	if err := db.Create(&revisionDistinctUser{Name: "first", CreatedAt: day1, UpdatedAt: day1}).Error; err != nil {
		t.Fatalf("create first shard row: %v", err)
	}
	if err := db.Create(&revisionDistinctUser{Name: "last", CreatedAt: day3, UpdatedAt: day3}).Error; err != nil {
		t.Fatalf("create last shard row: %v", err)
	}

	var users []revisionDistinctUser
	result := db.Where("created_at BETWEEN ? AND ?", day1, day3).
		Order("created_at ASC").
		Find(&users)
	if result.Error != nil {
		t.Fatalf("combined query with missing middle shard: %v", result.Error)
	}
	if len(users) != 2 || users[0].Name != "first" || users[1].Name != "last" {
		t.Fatalf("combined rows = %+v, want first and last rows", users)
	}
}

// TestCrossShardOrderLimitScan 验证 Scan 与 Find 一样支持跨分表排序和分页。
func TestCrossShardOrderLimitScan(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "low", Score: 10, CreatedAt: day1, UpdatedAt: day1},
		{Name: "high", Score: 20, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create scan rows: %v", err)
	}

	var users []requirementUser
	result := db.Model(&requirementUser{}).
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Order("score DESC").
		Limit(1).
		Scan(&users)
	if result.Error != nil {
		t.Fatalf("cross-shard scan with order and limit: %v", result.Error)
	}
	if len(users) != 1 || users[0].Name != "high" {
		t.Fatalf("scan rows = %+v, want high row only", users)
	}
}

// TestInitializeRejectsDuplicateLogicalTableName 验证不同模型不能注册为同一个逻辑表名。
func TestInitializeRejectsDuplicateLogicalTableName(t *testing.T) {
	plugin := New()
	for _, model := range []interface{}{revisionDuplicateUser{}, revisionDuplicateOrder{}} {
		if err := plugin.Register(model, ShardingConfig{
			ShardingKey:   "created_at",
			Strategy:      DayStrategy,
			MaxScanTables: 1,
		}); err != nil {
			t.Fatalf("register model: %v", err)
		}
	}

	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	if err := plugin.Initialize(db); err == nil {
		t.Fatal("initialize with duplicate logical table name returned nil")
	}
}

// TestInitializeRejectsGoFieldNameAsShardingKey 验证 ShardingKey 只能填写数据库列名。
func TestInitializeRejectsGoFieldNameAsShardingKey(t *testing.T) {
	plugin := New()
	if err := plugin.Register(revisionDistinctUser{}, ShardingConfig{
		ShardingKey:   "CreatedAt",
		Strategy:      DayStrategy,
		MaxScanTables: 1,
	}); err != nil {
		t.Fatalf("register model: %v", err)
	}

	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	if err := plugin.Initialize(db); err == nil {
		t.Fatal("initialize with Go field name sharding key returned nil")
	}
}

// TestCombinedQueryRetriesWithOneRemainingShard 验证 1146 恢复后只剩一张表时退回普通单表查询。
func TestCombinedQueryRetriesWithOneRemainingShard(t *testing.T) {
	prefix := revisionDistinctUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 3, revisionDistinctUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day3 := day1.AddDate(0, 0, 2)
	if err := db.Create(&revisionDistinctUser{Name: "only", CreatedAt: day1, UpdatedAt: day1}).Error; err != nil {
		t.Fatalf("create only shard row: %v", err)
	}

	var users []revisionDistinctUser
	result := db.Where("created_at BETWEEN ? AND ?", day1, day3).Order("created_at ASC").Find(&users)
	if result.Error != nil {
		t.Fatalf("combined query with one remaining shard: %v", result.Error)
	}
	if len(users) != 1 || users[0].Name != "only" {
		t.Fatalf("combined rows = %+v, want only row", users)
	}
}

// TestUpdateRejectsShardingKey 验证 GORM 更新不能修改分表字段，避免记录留在错误物理分表。
func TestUpdateRejectsShardingKey(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	user := requirementUser{Name: "locked", CreatedAt: day1, UpdatedAt: day1}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	result := db.Model(&requirementUser{}).
		Where("id = ? AND created_at = ?", user.ID, day1).
		Updates(map[string]interface{}{"created_at": day2})
	if result.Error == nil {
		t.Fatal("update sharding key returned nil")
	}
}

// TestUpdateRejectsShardingKeyGoFieldName 验证 GORM Go 字段名同样不能绕过分表字段更新限制。
func TestUpdateRejectsShardingKeyGoFieldName(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	user := requirementUser{Name: "go-field", CreatedAt: day1, UpdatedAt: day1}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	result := db.Model(&requirementUser{}).
		Where("id = ? AND created_at = ?", user.ID, day1).
		Updates(map[string]interface{}{"CreatedAt": day2})
	if result.Error == nil {
		t.Fatal("update sharding key with Go field name returned nil")
	}
}

// TestPrimaryKeyWriteRequiresShardingKey 验证仅按主键的 Update/Delete 不会扫描多个可能重复主键的分表。
func TestPrimaryKeyWriteRequiresShardingKey(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	user := requirementUser{Name: "primary-key", Score: 1, CreatedAt: day, UpdatedAt: day}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}

	update := db.Model(&requirementUser{}).Where("id = ?", user.ID).Updates(map[string]interface{}{"score": 2})
	if update.Error == nil || update.Error.Error() != "gorm_sharding: primary key update requires sharding key created_at" {
		t.Fatalf("primary key update error = %v", update.Error)
	}
	deleteResult := db.Delete(&requirementUser{}, user.ID)
	if deleteResult.Error == nil || deleteResult.Error.Error() != "gorm_sharding: primary key delete requires sharding key created_at" {
		t.Fatalf("primary key delete error = %v", deleteResult.Error)
	}

	update = db.Model(&requirementUser{}).
		Where("id = ? AND created_at = ?", user.ID, day).
		Updates(map[string]interface{}{"score": 2})
	if update.Error != nil || update.RowsAffected != 1 {
		t.Fatalf("primary key update with sharding key result = %+v", update)
	}
}

// TestCreateRejectsMissingShardingKey 验证 Create 不能通过 Select 或 Omit 省略分表字段。
func TestCreateRejectsMissingShardingKey(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	user := requirementUser{Name: "omit-sharding-key", CreatedAt: day, UpdatedAt: day}
	if result := db.Omit("CreatedAt").Create(&user); result.Error == nil {
		t.Fatal("create omitting sharding key returned nil")
	}
	if result := db.Select("name", "updated_at").Create(&user); result.Error == nil {
		t.Fatal("create selecting no sharding key returned nil")
	}
}

// TestCreateOnConflictRejectsShardingKey 验证冲突更新不能修改分表字段。
func TestCreateOnConflictRejectsShardingKey(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	user := requirementUser{Name: "upsert-sharding-key", CreatedAt: day, UpdatedAt: day}
	result := db.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"CreatedAt"})}).Create(&user)
	if result.Error == nil {
		t.Fatal("create on conflict updating sharding key returned nil")
	}
}

// TestCrossShardQueryRejectsSubquery 验证跨分表组合查询拒绝子查询，避免改写限定列后改变语义。
func TestCrossShardQueryRejectsSubquery(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "first", CreatedAt: day1, UpdatedAt: day1},
		{Name: "second", CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create subquery rows: %v", err)
	}

	var users []requirementUser
	result := db.Model(&requirementUser{}).
		Select("gs_req_user.*, (SELECT COUNT(*) FROM orders WHERE orders.user_id = gs_req_user.id) AS order_count").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Find(&users)
	if result.Error == nil || result.Error.Error() != "gorm_sharding: subquery across shards is not supported" {
		t.Fatalf("cross-shard subquery error = %v", result.Error)
	}
}

// TestCrossShardQueryRejectsPreload 验证跨分表查询不会对分表关联静默执行不完整的预加载。
func TestCrossShardQueryRejectsPreload(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "first", CreatedAt: day1, UpdatedAt: day1},
		{Name: "second", CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create preload rows: %v", err)
	}

	var users []requirementUser
	result := db.Preload("Orders").Where("created_at BETWEEN ? AND ?", day1, day2).Find(&users)
	if result.Error == nil || result.Error.Error() != "gorm_sharding: preload across shards is not supported" {
		t.Fatalf("cross-shard preload error = %v", result.Error)
	}
}

// TestSaveRejectsShardingKey 验证 Save 不会隐式把记录更新到错误的物理分表。
func TestSaveRejectsShardingKey(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	user := requirementUser{Name: "save", CreatedAt: day1, UpdatedAt: day1}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	user.CreatedAt = day2
	if result := db.Save(&user); result.Error == nil {
		t.Fatal("save with changed sharding key returned nil")
	}
}

// TestRawUpdateRejectsShardingKey 验证 Raw UPDATE 不能修改分表字段。
func TestRawUpdateRejectsShardingKey(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&requirementUser{Name: "raw-update", CreatedAt: day1, UpdatedAt: day1}).Error; err != nil {
		t.Fatalf("create user: %v", err)
	}
	result := db.Exec("UPDATE gs_req_user SET created_at = ? WHERE created_at = ?", day2, day1)
	if result.Error == nil {
		t.Fatal("raw update with changed sharding key returned nil")
	}
}

// TestRawInsertIntoLogicalTableIsRejected 验证 Raw INSERT 不会把历史数据静默写进最新分表。
func TestRawInsertIntoLogicalTableIsRejected(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	now := time.Now()
	if err := db.Create(&requirementUser{Name: "current", CreatedAt: now, UpdatedAt: now}).Error; err != nil {
		t.Fatalf("create current row: %v", err)
	}
	result := db.Exec(
		"INSERT INTO gs_req_user (name, score, created_at, updated_at) VALUES (?, ?, ?, ?)",
		"historical", 1, now.AddDate(0, 0, -7), now,
	)
	if result.Error == nil {
		t.Fatal("raw insert into logical table returned nil")
	}
}

// TestCrossShardDistinctFindIsGlobal 验证普通 Distinct Find 在全部命中分表上统一去重。
func TestCrossShardDistinctFindIsGlobal(t *testing.T) {
	prefix := revisionDistinctUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, revisionDistinctUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]revisionDistinctUser{
		{Name: "same", CreatedAt: day1, UpdatedAt: day1},
		{Name: "same", CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create distinct rows: %v", err)
	}

	var names []string
	result := db.Model(&revisionDistinctUser{}).
		Distinct("name").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Find(&names)
	if result.Error != nil {
		t.Fatalf("cross-shard distinct find: %v", result.Error)
	}
	if len(names) != 1 || names[0] != "same" {
		t.Fatalf("distinct names = %v, want [same]", names)
	}
}

// TestCrossShardCountIgnoresLimit 验证跨分表 COUNT 不会被明细分页截断。
func TestCrossShardCountIgnoresLimit(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "a", Score: 1, CreatedAt: day1, UpdatedAt: day1},
		{Name: "b", Score: 2, CreatedAt: day1, UpdatedAt: day1},
		{Name: "c", Score: 3, CreatedAt: day2, UpdatedAt: day2},
		{Name: "d", Score: 4, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create count rows: %v", err)
	}

	var count int64
	result := db.Model(&requirementUser{}).
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Limit(1).
		Count(&count)
	if result.Error != nil {
		t.Fatalf("cross-shard count with limit: %v", result.Error)
	}
	if count != 4 {
		t.Fatalf("cross-shard count = %d, want 4", count)
	}
}

// TestCrossShardDistinctLimitKeepsUniqueRows 验证跨分表 DISTINCT 分页不会被分表内重复值截断。
func TestCrossShardDistinctLimitKeepsUniqueRows(t *testing.T) {
	prefix := revisionDistinctUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, revisionDistinctUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]revisionDistinctUser{
		{Name: "a", CreatedAt: day1, UpdatedAt: day1},
		{Name: "a", CreatedAt: day1, UpdatedAt: day1},
		{Name: "b", CreatedAt: day1, UpdatedAt: day1},
		{Name: "c", CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create distinct limit rows: %v", err)
	}

	var names []string
	result := db.Model(&revisionDistinctUser{}).
		Distinct("name").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Order("name ASC").
		Limit(2).
		Find(&names)
	if result.Error != nil {
		t.Fatalf("cross-shard distinct limit: %v", result.Error)
	}
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("distinct names = %v, want [a b]", names)
	}
}

// TestCrossShardAggregateStats 验证 SUM、MIN、MAX、AVG 在多个分表上的最终结果由 Go 正确合并。
func TestCrossShardAggregateStats(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "a", Score: 1, CreatedAt: day1, UpdatedAt: day1},
		{Name: "b", Score: 2, CreatedAt: day1, UpdatedAt: day1},
		{Name: "c", Score: 10, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create aggregate rows: %v", err)
	}

	type stats struct {
		Total int64
		Min   int
		Max   int
		Avg   float64
	}
	var got stats
	result := db.Model(&requirementUser{}).
		Select("SUM(score) AS total, MIN(score) AS min, MAX(score) AS max, AVG(score) AS avg").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Find(&got)
	if result.Error != nil {
		t.Fatalf("cross-shard aggregate: %v", result.Error)
	}
	if got.Total != 13 || got.Min != 1 || got.Max != 10 || got.Avg != 13.0/3.0 {
		t.Fatalf("aggregate = %+v, want total=13 min=1 max=10 avg=%v", got, 13.0/3.0)
	}
}

// TestCrossShardGroupBy 验证跨分表分组由 MySQL 全局执行并返回正确结果。
func TestCrossShardGroupBy(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "alice", Score: 1, CreatedAt: day1, UpdatedAt: day1},
		{Name: "alice", Score: 2, CreatedAt: day2, UpdatedAt: day2},
		{Name: "bob", Score: 3, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create group rows: %v", err)
	}

	type group struct {
		Name  string
		Total int64
	}
	var got []group
	result := db.Model(&requirementUser{}).
		Select("name, COUNT(*) AS total").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Group("name").
		Order("name ASC").
		Find(&got)
	if result.Error != nil {
		t.Fatalf("cross-shard group: %v", result.Error)
	}
	if len(got) != 2 || got[0].Name != "alice" || got[0].Total != 2 || got[1].Name != "bob" || got[1].Total != 1 {
		t.Fatalf("groups = %+v, want alice=2 and bob=1", got)
	}
}

// TestCrossShardScanGroupAggregateSemantics 验证 Scan、HAVING、COUNT(DISTINCT) 与 AVG
// 由 MySQL 在全部分表原始行上统一计算，结果与单表 SQL 语义一致。
func TestCrossShardScanGroupAggregateSemantics(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "alice", Score: 7, CreatedAt: day1, UpdatedAt: day1},
		{Name: "alice", Score: 7, CreatedAt: day2, UpdatedAt: day2},
		{Name: "alice", Score: 9, CreatedAt: day2, UpdatedAt: day2},
		{Name: "bob", Score: 3, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create aggregate semantic rows: %v", err)
	}

	type groupStats struct {
		Name          string
		Total         int64
		DistinctScore int64
		Sum           int64
		Min           int
		Max           int
		Avg           float64
	}
	var got []groupStats
	result := db.Model(&requirementUser{}).
		Select("name, COUNT(*) AS total, COUNT(DISTINCT score) AS distinct_score, SUM(score) AS sum, MIN(score) AS min, MAX(score) AS max, AVG(score) AS avg").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Group("name").
		Having("COUNT(*) > ?", 2).
		Order("name ASC").
		Scan(&got)
	if result.Error != nil {
		t.Fatalf("cross-shard scan group aggregate: %v", result.Error)
	}
	if len(got) != 1 {
		t.Fatalf("group aggregate rows = %+v, want one alice row", got)
	}
	row := got[0]
	if row.Name != "alice" || row.Total != 3 || row.DistinctScore != 2 || row.Sum != 23 || row.Min != 7 || row.Max != 9 || math.Abs(row.Avg-23.0/3.0) > 0.0001 {
		t.Fatalf("group aggregate = %+v, want alice total=3 distinct=2 sum=23 min=7 max=9 avg=%v", row, 23.0/3.0)
	}
}

// TestCrossShardRowsUsesGlobalQuery 验证 Rows 也使用跨分表全局 SQL，而不是仅支持 Find 或 Scan。
func TestCrossShardRowsUsesGlobalQuery(t *testing.T) {
	prefix := revisionDistinctUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, revisionDistinctUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]revisionDistinctUser{
		{Name: "first", CreatedAt: day1, UpdatedAt: day1},
		{Name: "second", CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create rows query data: %v", err)
	}

	rows, err := db.Model(&revisionDistinctUser{}).
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Order("name ASC").
		Rows()
	if err != nil {
		t.Fatalf("cross-shard Rows: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var user revisionDistinctUser
		if err := db.ScanRows(rows, &user); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		names = append(names, user.Name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rows: %v", err)
	}
	if !sameStrings(names, []string{"first", "second"}) {
		t.Fatalf("rows names = %v, want [first second]", names)
	}
}

// TestRawUpdateAcrossShards 验证 Raw UPDATE 会逐张执行命中的真实分表并累加影响行数。
func TestRawUpdateAcrossShards(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "first", Score: 1, CreatedAt: day1, UpdatedAt: day1},
		{Name: "second", Score: 2, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create raw update rows: %v", err)
	}

	result := db.Exec("UPDATE gs_req_user SET score = ? WHERE created_at BETWEEN ? AND ?", 9, day1, day2)
	if result.Error != nil {
		t.Fatalf("raw update across shards: %v", result.Error)
	}
	if result.RowsAffected != 2 {
		t.Fatalf("raw update RowsAffected = %d, want 2", result.RowsAffected)
	}
	var users []requirementUser
	if err := db.Where("created_at BETWEEN ? AND ?", day1, day2).Order("name ASC").Find(&users).Error; err != nil {
		t.Fatalf("find updated rows: %v", err)
	}
	if len(users) != 2 || users[0].Score != 9 || users[1].Score != 9 {
		t.Fatalf("updated rows = %+v, want both scores 9", users)
	}
}

// TestRawUpdateAcrossShardsSkipsMissingShard 验证 1146 缺失分表按空表跳过，不影响其他分表写入。
func TestRawUpdateAcrossShardsSkipsMissingShard(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "first", Score: 1, CreatedAt: day1, UpdatedAt: day1},
		{Name: "second", Score: 2, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create rollback rows: %v", err)
	}
	missingTable := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy}.tableName(day1)
	if err := rawDB.Migrator().DropTable(missingTable); err != nil {
		t.Fatalf("drop second raw update shard: %v", err)
	}

	result := db.Exec("UPDATE gs_req_user SET score = ? WHERE created_at BETWEEN ? AND ?", 9, day1, day2)
	if result.Error != nil {
		t.Fatalf("raw update with missing shard: %v", result.Error)
	}
	remainingTable := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy}.tableName(day2)
	var score int
	if err := rawDB.Table(remainingTable).Select("score").Where("name = ?", "second").Scan(&score).Error; err != nil {
		t.Fatalf("read row after missing shard: %v", err)
	}
	if score != 9 {
		t.Fatalf("score after missing shard = %d, want 9", score)
	}
}

// TestRawUpdateAcrossShardsUsesOuterTransaction 验证外层事务由调用方决定提交或回滚。
func TestRawUpdateAcrossShardsUsesOuterTransaction(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "first", Score: 1, CreatedAt: day1, UpdatedAt: day1},
		{Name: "second", Score: 2, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create outer transaction rows: %v", err)
	}

	tx := db.Begin()
	if tx.Error != nil {
		t.Fatalf("begin transaction: %v", tx.Error)
	}
	result := tx.Exec("UPDATE gs_req_user SET score = ? WHERE created_at BETWEEN ? AND ?", 9, day1, day2)
	if result.Error != nil {
		tx.Rollback()
		t.Fatalf("raw update in outer transaction: %v", result.Error)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("rollback outer transaction: %v", err)
	}
	for _, check := range []struct {
		table string
		name  string
		want  int
	}{
		{ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy}.tableName(day1), "first", 1},
		{ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy}.tableName(day2), "second", 2},
	} {
		var score int
		if err := rawDB.Table(check.table).Select("score").Where("name = ?", check.name).Scan(&score).Error; err != nil {
			t.Fatalf("read %s after outer rollback: %v", check.name, err)
		}
		if score != check.want {
			t.Fatalf("%s score after outer rollback = %d, want %d", check.name, score, check.want)
		}
	}
}

// TestRawDeleteAcrossShards 验证 Raw DELETE 会逐张删除命中的真实分表记录。
func TestRawDeleteAcrossShards(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	if err := db.Create(&[]requirementUser{
		{Name: "first", Score: 1, CreatedAt: day1, UpdatedAt: day1},
		{Name: "second", Score: 2, CreatedAt: day2, UpdatedAt: day2},
	}).Error; err != nil {
		t.Fatalf("create raw delete rows: %v", err)
	}

	result := db.Exec("DELETE FROM gs_req_user WHERE created_at BETWEEN ? AND ?", day1, day2)
	if result.Error != nil {
		t.Fatalf("raw delete across shards: %v", result.Error)
	}
	if result.RowsAffected != 2 {
		t.Fatalf("raw delete RowsAffected = %d, want 2", result.RowsAffected)
	}
	var count int64
	if err := db.Model(&requirementUser{}).Where("created_at BETWEEN ? AND ?", day1, day2).Count(&count).Error; err != nil {
		t.Fatalf("count deleted rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("rows after raw delete = %d, want 0", count)
	}
}
