package gorm_sharding

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// requirementUser 是需求验收测试使用的业务模型。
type requirementUser struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name;type:varchar(64);not null;default:'';index"`
	Score     int       `gorm:"column:score;not null;default:0;index"`
	Score2    int       `gorm:"column:score2;not null;default:0;index"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime;not null;index"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime;not null"`
}

// TableName 返回测试用户表逻辑表名，插件会用它作为分表前缀。
func (requirementUser) TableName() string {
	return "gs_req_user"
}

// requirementUserV1 模拟历史表创建时的旧结构。
type requirementUserV1 struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name;type:varchar(64);not null;default:'';index"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime;not null;index"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime;not null"`
}

// TableName 返回迁移测试旧模型逻辑表名。
func (requirementUserV1) TableName() string {
	return "gs_req_migrate_user"
}

// requirementUserV2 模拟业务模型新增 Age 字段后的最新结构。
type requirementUserV2 struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name;type:varchar(64);not null;default:'';index"`
	Age       int       `gorm:"column:age;not null;default:0"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime;not null;index"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime;not null"`
}

// TableName 返回迁移测试新模型逻辑表名，必须和旧模型一致才能迁移同一批历史分表。
func (requirementUserV2) TableName() string {
	return "gs_req_migrate_user"
}

// TestRequirementStrategies 验证需求文档列出的年月周日小时分表后缀。
func TestRequirementStrategies(t *testing.T) {
	cases := []struct {
		name     string
		strategy ShardingStrategy
		at       time.Time
		want     string
	}{
		{name: "year", strategy: YearStrategy, at: time.Date(2026, 8, 4, 13, 0, 0, 0, time.Local), want: "2026"},
		{name: "month", strategy: MonthStrategy, at: time.Date(2026, 8, 4, 13, 0, 0, 0, time.Local), want: "202608"},
		{name: "week", strategy: WeekStrategy, at: time.Date(2026, 8, 4, 13, 0, 0, 0, time.Local), want: "2026_w32"},
		{name: "day", strategy: DayStrategy, at: time.Date(2026, 8, 4, 13, 0, 0, 0, time.Local), want: "20260804"},
		{name: "hour", strategy: HourStrategy, at: time.Date(2026, 8, 4, 13, 0, 0, 0, time.Local), want: "2026080413"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.strategy.Suffix(tt.at); got != tt.want {
				t.Fatalf("suffix = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRequirementCRUDAndRaw 验证建表、插入、批量拆分、查询、更新、删除、Raw 和 RowsAffected 行为。
func TestRequirementCRUDAndRaw(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	base := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	oldAt := base.AddDate(0, 0, -2)
	midAt := base.AddDate(0, 0, -1)
	newAt := base
	users := []requirementUser{
		{Name: "scan_old", CreatedAt: oldAt, UpdatedAt: oldAt},
		{Name: "scan_mid", CreatedAt: midAt, UpdatedAt: midAt},
		{Name: "scan_new", CreatedAt: newAt, UpdatedAt: newAt},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("batch create split across shards failed: %v", err)
	}
	for index, user := range users {
		if user.ID == 0 {
			t.Fatalf("batch create user %d ID was not filled", index)
		}
	}

	cfg := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy, MaxScanTables: 2}
	if tableExists(t, rawDB, prefix) {
		t.Fatalf("logical template table %s was created", prefix)
	}
	for _, at := range []time.Time{oldAt, midAt, newAt} {
		table := cfg.tableName(at)
		if !tableExists(t, rawDB, table) {
			t.Fatalf("expected shard table %s to exist", table)
		}
		if got := countRows(t, rawDB, table, "1=1"); got != 1 {
			t.Fatalf("rows in %s = %d, want 1", table, got)
		}
	}

	var ranged []requirementUser
	if err := db.Where("created_at BETWEEN ? AND ?", oldAt, midAt).Find(&ranged).Error; err != nil {
		t.Fatalf("range query failed: %v", err)
	}
	if len(ranged) != 2 {
		t.Fatalf("range query rows = %d, want 2", len(ranged))
	}

	var recent []requirementUser
	if err := db.Where("name LIKE ?", "scan_%").Find(&recent).Error; err != nil {
		t.Fatalf("max scan query failed: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("query without sharding key scanned %d rows, want newest 2 rows", len(recent))
	}

	var rawUsers []requirementUser
	if err := db.Raw("SELECT * FROM "+prefix+" WHERE name = ?", "scan_new").Scan(&rawUsers).Error; err != nil {
		t.Fatalf("raw single-table sql rewrite failed: %v", err)
	}
	if len(rawUsers) != 1 || rawUsers[0].Name != "scan_new" {
		t.Fatalf("raw rows = %+v, want scan_new only", rawUsers)
	}

	res := db.Model(&requirementUser{}).
		Where("created_at = ?", oldAt).
		Updates(map[string]interface{}{"score": 11})
	if res.Error != nil {
		t.Fatalf("exact update failed: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("exact update RowsAffected = %d, want 1", res.RowsAffected)
	}
	if got := countRows(t, rawDB, cfg.tableName(oldAt), "score = 11"); got != 1 {
		t.Fatalf("exact update did not update old shard")
	}

	res = db.Model(&requirementUser{}).
		Where("name LIKE ?", "scan_%").
		Updates(map[string]interface{}{"score2": 22})
	if res.Error != nil {
		t.Fatalf("scan update failed: %v", res.Error)
	}
	if res.RowsAffected != 2 {
		t.Fatalf("scan update RowsAffected = %d, want 2", res.RowsAffected)
	}
	if got := countRows(t, rawDB, cfg.tableName(oldAt), "score2 = 22"); got != 0 {
		t.Fatalf("scan update touched old shard rows = %d, want 0", got)
	}

	res = db.Where("created_at = ?", midAt).Delete(&requirementUser{})
	if res.Error != nil {
		t.Fatalf("exact delete failed: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("exact delete RowsAffected = %d, want 1", res.RowsAffected)
	}
	if got := countRows(t, rawDB, cfg.tableName(midAt), "name = 'scan_mid'"); got != 0 {
		t.Fatalf("exact delete left %d rows in mid shard", got)
	}

	res = db.Where("name LIKE ?", "scan_%").Delete(&requirementUser{})
	if res.Error != nil {
		t.Fatalf("scan delete failed: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("scan delete RowsAffected = %d, want 1", res.RowsAffected)
	}
	if got := countRows(t, rawDB, cfg.tableName(oldAt), "name = 'scan_old'"); got != 1 {
		t.Fatalf("scan delete should keep old shard outside MaxScanTables, got %d rows", got)
	}
}

// TestRequirementCreateInBatchesAcrossShards 验证 GORM CreateInBatches 跨分表插入时仍能正确拆表、建表和统计影响行数。
func TestRequirementCreateInBatchesAcrossShards(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 3, requirementUser{})
	defer cleanup()

	base := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	users := []requirementUser{
		{Name: "batch_1", CreatedAt: base.AddDate(0, 0, -2), UpdatedAt: base},
		{Name: "batch_2", CreatedAt: base.AddDate(0, 0, -1), UpdatedAt: base},
		{Name: "batch_3", CreatedAt: base, UpdatedAt: base},
		{Name: "batch_4", CreatedAt: base.AddDate(0, 0, -1), UpdatedAt: base},
		{Name: "batch_5", CreatedAt: base, UpdatedAt: base},
	}
	res := db.CreateInBatches(&users, 2)
	if res.Error != nil {
		t.Fatalf("CreateInBatches across shards failed: %v", res.Error)
	}
	if res.RowsAffected != int64(len(users)) {
		t.Fatalf("CreateInBatches RowsAffected = %d, want %d", res.RowsAffected, len(users))
	}

	cfg := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy, MaxScanTables: 3}
	for _, tt := range []struct {
		at   time.Time
		want int64
	}{
		{at: base.AddDate(0, 0, -2), want: 1},
		{at: base.AddDate(0, 0, -1), want: 2},
		{at: base, want: 2},
	} {
		table := cfg.tableName(tt.at)
		if got := countRows(t, rawDB, table, "name LIKE 'batch_%'"); got != tt.want {
			t.Fatalf("rows in %s = %d, want %d", table, got, tt.want)
		}
	}
}

// TestRequirementArrayCRUDAcrossShards 验证指针数组可跨分表 Create、Updates、Delete，并回填自增主键。
func TestRequirementArrayCRUDAcrossShards(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	first := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	users := [2]requirementUser{
		{Name: "array_first", Score: 1, CreatedAt: first, UpdatedAt: first},
		{Name: "array_second", Score: 2, CreatedAt: first.AddDate(0, 0, 1), UpdatedAt: first.AddDate(0, 0, 1)},
	}
	if result := db.Create(&users); result.Error != nil || result.RowsAffected != 2 {
		t.Fatalf("array create = rows:%d err:%v", result.RowsAffected, result.Error)
	}
	for index, user := range users {
		if user.ID == 0 {
			t.Fatalf("array create user %d ID was not filled", index)
		}
	}

	if result := db.Model(&users).Updates(map[string]interface{}{"score": 100}); result.Error != nil || result.RowsAffected != 2 {
		t.Fatalf("array update = rows:%d err:%v", result.RowsAffected, result.Error)
	}
	for _, user := range users {
		var score int
		if err := rawDB.Table(ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy}.tableName(user.CreatedAt)).
			Where("name = ?", user.Name).Select("score").Scan(&score).Error; err != nil || score != 100 {
			t.Fatalf("array update user %s = score:%d err:%v", user.Name, score, err)
		}
	}

	if result := db.Delete(&users); result.Error != nil || result.RowsAffected != 2 {
		t.Fatalf("array delete = rows:%d err:%v", result.RowsAffected, result.Error)
	}
	for _, user := range users {
		table := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy}.tableName(user.CreatedAt)
		if got := countRows(t, rawDB, table, "name = '"+user.Name+"'"); got != 0 {
			t.Fatalf("array delete rows in %s = %d, want 0", table, got)
		}
	}
}

// TestRequirementPreciseWriteWithAdditionalPredicates 验证包含分表字段和其他条件时仍然精确路由目标表。
func TestRequirementPreciseWriteWithAdditionalPredicates(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	base := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	oldAt := base.AddDate(0, 0, -2)
	for _, user := range []requirementUser{
		{Name: "target", CreatedAt: oldAt, UpdatedAt: oldAt},
		{Name: "recent_1", CreatedAt: base.AddDate(0, 0, -1), UpdatedAt: base.AddDate(0, 0, -1)},
		{Name: "recent_2", CreatedAt: base, UpdatedAt: base},
	} {
		if err := db.Create(&user).Error; err != nil {
			t.Fatalf("create %s failed: %v", user.Name, err)
		}
	}

	res := db.Model(&requirementUser{}).
		Where("created_at = ? AND name = ?", oldAt, "target").
		Updates(map[string]interface{}{"score": 33})
	if res.Error != nil {
		t.Fatalf("compound precise update failed: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("compound precise update RowsAffected = %d, want 1", res.RowsAffected)
	}

	table := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy, MaxScanTables: 2}.tableName(oldAt)
	if got := countRows(t, rawDB, table, "name = 'target' AND score = 33"); got != 1 {
		t.Fatalf("compound precise update did not hit old shard, rows = %d", got)
	}

	res = db.Where("created_at = ? AND name = ?", oldAt, "target").Delete(&requirementUser{})
	if res.Error != nil {
		t.Fatalf("compound precise delete failed: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("compound precise delete RowsAffected = %d, want 1", res.RowsAffected)
	}
}

// TestRequirementUnsupportedCrossShardQueries 验证跨分表 Join 仍然不支持。
func TestRequirementUnsupportedCrossShardQueries(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	start := time.Date(2026, 8, 3, 0, 0, 0, 0, time.Local)
	end := time.Date(2026, 8, 4, 0, 0, 0, 0, time.Local)
	var users []requirementUser
	if err := db.Joins("JOIN gs_req_other ON gs_req_other.id = id").
		Where("created_at BETWEEN ? AND ?", start, end).
		Find(&users).Error; err == nil {
		t.Fatal("expected unsupported join query to return error")
	}
}

// TestRequirementCrossShardOrderLimitAndGroupBy 验证跨分表全局排序分页和聚合由 MySQL 外层 SQL 执行。
func TestRequirementCrossShardOrderLimitAndGroupBy(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, _, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	day1 := time.Date(2026, 8, 2, 10, 0, 0, 0, time.Local)
	day2 := day1.AddDate(0, 0, 1)
	users := []requirementUser{
		{Name: "alice", Score: 10, CreatedAt: day1, UpdatedAt: day1},
		{Name: "alice", Score: 30, CreatedAt: day2, UpdatedAt: day2},
		{Name: "bob", Score: 20, CreatedAt: day2, UpdatedAt: day2},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("create cross-shard users: %v", err)
	}

	var ordered []requirementUser
	if err := db.Where("created_at BETWEEN ? AND ?", day1, day2).
		Order("score DESC").
		Offset(1).
		Limit(1).
		Find(&ordered).Error; err != nil {
		t.Fatalf("cross-shard order and limit: %v", err)
	}
	if len(ordered) != 1 || ordered[0].Score != 20 {
		t.Fatalf("ordered rows = %+v, want score 20 only", ordered)
	}

	type nameCount struct {
		Name  string
		Total int64
	}
	var groups []nameCount
	if err := db.Model(&requirementUser{}).
		Select("name, COUNT(*) AS total").
		Where("created_at BETWEEN ? AND ?", day1, day2).
		Group("name").
		Order("name ASC").
		Find(&groups).Error; err != nil {
		t.Fatalf("cross-shard group by: %v", err)
	}
	if len(groups) != 2 || groups[0].Name != "alice" || groups[0].Total != 2 || groups[1].Name != "bob" || groups[1].Total != 1 {
		t.Fatalf("group rows = %+v, want alice=2 and bob=1", groups)
	}
}

// TestRequirementMissingTableInvalidatesAfterSQL 验证先执行 SQL，遇到 1146 后再清缓存并按表不存在处理。
func TestRequirementMissingTableInvalidatesAfterSQL(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 3, requirementUser{})
	defer cleanup()

	now := time.Date(2026, 8, 4, 10, 0, 0, 0, time.Local)
	if err := db.Create(&requirementUser{Name: "gone", CreatedAt: now, UpdatedAt: now}).Error; err != nil {
		t.Fatalf("create failed: %v", err)
	}
	var users []requirementUser
	if err := db.Where("name = ?", "gone").Find(&users).Error; err != nil {
		t.Fatalf("warm table-list cache failed: %v", err)
	}

	table := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy, MaxScanTables: 3}.tableName(now)
	if err := rawDB.Exec("DROP TABLE " + quoteIdent(table)).Error; err != nil {
		t.Fatalf("drop test shard table failed: %v", err)
	}

	res := db.Model(&requirementUser{}).Where("name = ?", "gone").Updates(map[string]interface{}{"score": 99})
	if res.Error != nil {
		t.Fatalf("missing table should be handled after SQL execution, got error: %v", res.Error)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("missing table update RowsAffected = %d, want 0", res.RowsAffected)
	}
}

// TestRequirementBoundaryRefreshesTableListCache 验证切到新分片时不会继续使用旧分片的最近表列表缓存。
func TestRequirementBoundaryRefreshesTableListCache(t *testing.T) {
	prefix := "gs_req_boundary_user"
	rawDB := openRequirementDB(t)
	defer closeRequirementDB(t, rawDB)
	cleanupRequirementPrefix(t, rawDB, prefix)
	defer cleanupRequirementPrefix(t, rawDB, prefix)

	cfg := ShardingConfig{
		ShardingKey:     "created_at",
		Strategy:        HourStrategy,
		tablePrefix:     prefix,
		MaxScanTables:   2,
		AutoCreateTable: true,
		AutoMigrate:     true,
	}
	manager := newTableManager(rawDB)
	oldHour := time.Date(2026, 8, 4, 16, 0, 0, 0, time.Local)
	newHour := oldHour.Add(time.Hour)
	oldTables := []string{cfg.tableName(oldHour), cfg.tableName(oldHour.Add(-time.Hour))}
	for _, table := range oldTables {
		if err := rawDB.Table(table).AutoMigrate(&requirementUser{}); err != nil {
			t.Fatalf("create old shard %s failed: %v", table, err)
		}
	}

	oldList := manager.tables(rawDB, cfg, oldHour)
	if !sameStrings(oldList, oldTables) {
		t.Fatalf("old shard list = %v, want %v", oldList, oldTables)
	}

	newTable := cfg.tableName(newHour)
	warmedBeforeCreate := manager.tables(rawDB, cfg, newHour)
	if containsString(warmedBeforeCreate, newTable) {
		t.Fatalf("new shard %s should not be listed before it is created", newTable)
	}

	if err := manager.ensure(rawDB, &requirementUser{}, cfg, newTable); err != nil {
		t.Fatalf("ensure new shard at boundary failed: %v", err)
	}
	afterCreate := manager.tables(rawDB, cfg, newHour)
	if len(afterCreate) == 0 || afterCreate[0] != newTable {
		t.Fatalf("new shard list after boundary create = %v, want first table %s", afterCreate, newTable)
	}
	if !containsString(afterCreate, oldTables[0]) {
		t.Fatalf("new shard list after boundary create = %v, want to keep recent old shard %s", afterCreate, oldTables[0])
	}
}

// TestRequirementCreateNewShardInsideTransaction 验证事务内首次建表不会提交业务写入。
func TestRequirementCreateNewShardInsideTransaction(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	createdAt := time.Date(2026, 8, 6, 10, 0, 0, 0, time.Local)
	table := ShardingConfig{tablePrefix: prefix, Strategy: DayStrategy}.tableName(createdAt)
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatalf("begin transaction: %v", tx.Error)
	}
	if err := tx.Create(&requirementUser{Name: "rollback", CreatedAt: createdAt, UpdatedAt: createdAt}).Error; err != nil {
		tx.Rollback()
		t.Fatalf("create first shard row in transaction: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatalf("rollback transaction: %v", err)
	}
	if !tableExists(t, rawDB, table) {
		t.Fatalf("new shard %s was not created", table)
	}
	if got := countRows(t, rawDB, table, "1=1"); got != 0 {
		t.Fatalf("rows in rolled back new shard = %d, want 0", got)
	}
}

// TestRequirementMultiShardWriteUsesInternalTransaction 验证关闭默认事务时多分表写入仍会整体回滚。
func TestRequirementMultiShardWriteUsesInternalTransaction(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, DayStrategy, 2, requirementUser{})
	defer cleanup()

	table := prefix + "_internal_tx"
	if err := rawDB.Exec("CREATE TABLE " + quoteIdent(table) + " (id BIGINT PRIMARY KEY)").Error; err != nil {
		t.Fatalf("create internal transaction table: %v", err)
	}

	_, err := executeMultiShardWrite(db.Session(&gorm.Session{SkipDefaultTransaction: true}), func(tx *gorm.DB) (int64, error) {
		if err := tx.Exec("INSERT INTO " + quoteIdent(table) + " (id) VALUES (1)").Error; err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("force multi-shard write rollback")
	})
	if err == nil {
		t.Fatal("internal transaction returned nil error")
	}
	if got := countRows(t, rawDB, table, "1=1"); got != 0 {
		t.Fatalf("rows after internal transaction rollback = %d, want 0", got)
	}
}

// TestRequirementConcurrentCreateSameShard 验证切表瞬间并发首次写同一新分表时只暴露正常插入结果。
func TestRequirementConcurrentCreateSameShard(t *testing.T) {
	prefix := requirementUser{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, HourStrategy, 3, requirementUser{})
	defer cleanup()

	createdAt := time.Date(2026, 8, 5, 10, 0, 0, 0, time.Local)
	const workers = 20
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- db.Create(&requirementUser{
				Name:      fmt.Sprintf("worker_%02d", i),
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
			}).Error
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent create returned error: %v", err)
		}
	}
	table := ShardingConfig{tablePrefix: prefix, Strategy: HourStrategy, MaxScanTables: 3}.tableName(createdAt)
	if got := countRows(t, rawDB, table, "1=1"); got != workers {
		t.Fatalf("concurrent create rows = %d, want %d", got, workers)
	}
}

// TestRequirementRejectsDuplicateInitialize 验证同一个插件实例不能重复绑定多个 DB。
func TestRequirementRejectsDuplicateInitialize(t *testing.T) {
	plugin := New()
	if err := plugin.Register(requirementUser{}, ShardingConfig{
		ShardingKey:     "created_at",
		Strategy:        HourStrategy,
		MaxScanTables:   3,
		AutoCreateTable: true,
		AutoMigrate:     true,
	}); err != nil {
		t.Fatalf("register plugin failed: %v", err)
	}

	db1 := openRequirementDB(t)
	defer closeRequirementDB(t, db1)
	db2 := openRequirementDB(t)
	defer closeRequirementDB(t, db2)

	if err := db1.Use(plugin); err != nil {
		t.Fatalf("first plugin use failed: %v", err)
	}
	if err := db2.Use(plugin); err == nil {
		t.Fatalf("second plugin use should be rejected")
	}
}

// TestRequirementPluginAutoMigrateSyncsHistoricalTables 验证插件 AutoMigrate 能同步历史分表新字段。
func TestRequirementPluginAutoMigrateSyncsHistoricalTables(t *testing.T) {
	prefix := requirementUserV1{}.TableName()
	db, rawDB, cleanup := newRequirementShardedDB(t, prefix, MonthStrategy, 5, requirementUserV1{})
	defer cleanup()

	createdAt := time.Date(2026, 1, 2, 3, 0, 0, 0, time.Local)
	if err := db.Create(&requirementUserV1{Name: "old", CreatedAt: createdAt, UpdatedAt: createdAt}).Error; err != nil {
		t.Fatalf("create historical table failed: %v", err)
	}

	plugin := New()
	if err := plugin.Register(requirementUserV2{}, ShardingConfig{
		ShardingKey:     "created_at",
		Strategy:        MonthStrategy,
		MaxScanTables:   5,
		AutoCreateTable: true,
		AutoMigrate:     true,
	}); err != nil {
		t.Fatalf("register v2 failed: %v", err)
	}
	migrateDB := openRequirementDB(t)
	defer closeRequirementDB(t, migrateDB)
	if err := migrateDB.Use(plugin); err != nil {
		t.Fatalf("use plugin failed: %v", err)
	}
	if err := plugin.AutoMigrate(migrateDB, &requirementUserV2{}); err != nil {
		t.Fatalf("plugin AutoMigrate failed: %v", err)
	}

	table := ShardingConfig{tablePrefix: prefix, Strategy: MonthStrategy, MaxScanTables: 5}.tableName(createdAt)
	if !columnExists(t, rawDB, table, "age") {
		t.Fatalf("historical shard %s does not have migrated age column", table)
	}
}

// newRequirementShardedDB 创建测试用分表 DB，并在前后清理指定前缀的表。
func newRequirementShardedDB(t *testing.T, prefix string, strategy ShardingStrategy, maxScanTables int, model interface{}) (*gorm.DB, *gorm.DB, func()) {
	t.Helper()

	rawDB := openRequirementDB(t)
	cleanupRequirementPrefix(t, rawDB, prefix)

	plugin := New()
	if err := plugin.Register(model, ShardingConfig{
		ShardingKey:     "created_at",
		Strategy:        strategy,
		MaxScanTables:   maxScanTables,
		AutoCreateTable: true,
		AutoMigrate:     true,
	}); err != nil {
		t.Fatalf("register plugin failed: %v", err)
	}

	db := openRequirementDB(t)
	if err := db.Use(plugin); err != nil {
		t.Fatalf("use plugin failed: %v", err)
	}

	cleanup := func() {
		cleanupRequirementPrefix(t, rawDB, prefix)
		closeRequirementDB(t, db)
		closeRequirementDB(t, rawDB)
	}
	return db, rawDB, cleanup
}

// openRequirementDB 打开测试数据库；默认使用本机示例库，也可用 GORM_SHARDING_TEST_DSN 覆盖。
func openRequirementDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("GORM_SHARDING_TEST_DSN")
	if dsn == "" {
		dsn = "root:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("open mysql failed: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql db failed: %v", err)
	}
	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("ping mysql failed: %v", err)
	}
	return db
}

// closeRequirementDB 关闭测试数据库连接。
func closeRequirementDB(t *testing.T, db *gorm.DB) {
	t.Helper()

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql db for close failed: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close sql db failed: %v", err)
	}
}

// cleanupRequirementPrefix 删除本测试专用前缀下的表，避免污染其他业务表。
func cleanupRequirementPrefix(t *testing.T, db *gorm.DB, prefix string) {
	t.Helper()

	if !strings.HasPrefix(prefix, "gs_req_") {
		t.Fatalf("unsafe cleanup prefix %q", prefix)
	}
	var tables []string
	if err := db.Raw(
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE ?",
		prefix+"%",
	).Scan(&tables).Error; err != nil {
		t.Fatalf("list cleanup tables failed: %v", err)
	}
	dropTables(t, db, tables)
}

// dropTables 删除给定测试表名。
func dropTables(t *testing.T, db *gorm.DB, tables []string) {
	t.Helper()

	for _, table := range tables {
		if !strings.HasPrefix(table, "gs_req_") {
			t.Fatalf("unsafe drop table %q", table)
		}
		if err := db.Exec("DROP TABLE IF EXISTS " + quoteIdent(table)).Error; err != nil {
			t.Fatalf("drop table %s failed: %v", table, err)
		}
	}
}

// tableExists 判断测试表是否存在。
func tableExists(t *testing.T, db *gorm.DB, table string) bool {
	t.Helper()

	var count int64
	if err := db.Raw(
		"SELECT count(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		table,
	).Scan(&count).Error; err != nil {
		t.Fatalf("check table %s failed: %v", table, err)
	}
	return count > 0
}

// columnExists 判断测试表字段是否存在。
func columnExists(t *testing.T, db *gorm.DB, table string, column string) bool {
	t.Helper()

	var count int64
	if err := db.Raw(
		"SELECT count(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
		table,
		column,
	).Scan(&count).Error; err != nil {
		t.Fatalf("check column %s.%s failed: %v", table, column, err)
	}
	return count > 0
}

// countRows 返回指定测试表内满足条件的行数。
func countRows(t *testing.T, db *gorm.DB, table string, where string) int64 {
	t.Helper()

	var count int64
	if err := db.Table(table).Where(where).Count(&count).Error; err != nil {
		t.Fatalf("count %s failed: %v", table, err)
	}
	return count
}

// quoteIdent 安全引用测试生成的 MySQL 标识符。
func quoteIdent(name string) string {
	if !strings.HasPrefix(name, "gs_req_") {
		panic(fmt.Sprintf("unsafe identifier %q", name))
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// sameStrings 判断两个字符串切片内容和顺序是否一致。
func sameStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// containsString 判断字符串切片是否包含指定值。
func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
