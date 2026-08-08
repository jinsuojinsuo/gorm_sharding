package gorm_sharding

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

// tableManager 负责真实分表的存在性检查、创建、扫描和历史表迁移。
type tableManager struct {
	// ddlDB 保存插件初始化时的非事务连接。事务内首次建表必须使用它，
	// 避免 MySQL DDL 在当前事务连接上隐式提交业务 DML。
	ddlDB *gorm.DB

	// seen 缓存已经确认存在的真实分表，避免循环内重复 HasTable。
	seen sync.Map

	// tableLists 缓存“最近 N 个周期内存在的表”的查询结果；缓存键包含当前分片，跨分片时间后会自动失效。
	tableLists sync.Map

	// createGroup 合并同一真实分表的并发建表请求，避免切表瞬间重复 AutoMigrate。
	createGroup singleflight.Group
}

// newTableManager 创建表管理器实例。
func newTableManager(db *gorm.DB) *tableManager {
	return &tableManager{ddlDB: db.Session(&gorm.Session{NewDB: true})}
}

// exists 判断真实分表是否存在；已经确认存在的表会缓存，避免循环扫描时反复查 information_schema。
func (m *tableManager) exists(db *gorm.DB, table string) bool {
	if _, ok := m.seen.Load(table); ok {
		return true
	}

	// 不使用 Migrator().HasTable：它内部通过 Raw(...).Row() 查询当前数据库，
	// 会经过 Row 回调，在插件接管回调后无法安全嵌套。直接查询 information_schema
	// 既能复用当前事务连接，也不会修改业务 Statement。
	var count int64
	metadataDB := db.Session(&gorm.Session{NewDB: true})
	// 1146 恢复路径会携带上一次业务 SQL 的错误；元数据查询必须独立执行。
	metadataDB.Error = nil
	err := metadataDB.Raw(
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		table,
	).Scan(&count).Error
	if err != nil || count == 0 {
		return false
	}
	m.seen.Store(table, struct{}{})
	return true
}

// invalidate 删除指定真实分表的缓存；执行 SQL 时发现表被外部删除后调用。
func (m *tableManager) invalidate(cfg ShardingConfig, table string) {
	m.seen.Delete(table)
	m.clearTableListCache(cfg)
}

// existingAfterMissing 在 SQL 返回 1146 后刷新候选表缓存，并返回仍然存在的真实分表。
func (m *tableManager) existingAfterMissing(db *gorm.DB, cfg ShardingConfig, candidates []string) []string {
	for _, table := range candidates {
		m.seen.Delete(table)
	}
	m.clearTableListCache(cfg)

	existing := make([]string, 0, len(candidates))
	for _, table := range candidates {
		if m.exists(db, table) {
			existing = append(existing, table)
		}
	}
	return existing
}

// isMissingTableError 判断错误是否是 MySQL 1146 表不存在错误。
func isMissingTableError(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1146
}

// ensure 确保指定真实分表存在；开启 AutoCreateTable 时会用 GORM AutoMigrate 建表。
func (m *tableManager) ensure(db *gorm.DB, model interface{}, cfg ShardingConfig, table string) error {
	if !cfg.AutoCreateTable {
		return nil
	}
	if m.exists(db, table) {
		return nil
	}
	createDB := db
	if hasActiveTransaction(db) {
		// MySQL DDL 会隐式提交当前连接的事务。改用初始化时保存的非事务连接
		// 创建物理表，随后仍由原事务重试 INSERT，因此 Rollback 只保留空表。
		createDB = m.ddlDB.WithContext(db.Statement.Context)
	}
	_, err, _ := m.createGroup.Do(table, func() (interface{}, error) {
		if m.exists(db, table) {
			return nil, nil
		}
		// 不手写 CREATE TABLE，直接让 GORM AutoMigrate 根据 struct 和 tag 创建真实分表。
		if err := createDB.Session(&gorm.Session{NewDB: true}).Table(table).AutoMigrate(model); err != nil {
			return nil, err
		}
		m.seen.Store(table, struct{}{})
		m.clearTableListCache(cfg)
		// 只在当前周期首次建表后清理。补写历史数据时不能以历史时间为窗口锚点删除较新分表。
		now := cfg.shardTime(time.Now())
		if !hasActiveTransaction(db) && cfg.MaxRetainTables > 0 && table == cfg.tableName(now) {
			if err := m.cleanupExpiredTables(createDB, cfg, now); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

// cleanupExpiredTables 删除当前保留窗口之前、能被当前策略识别的真实分表。
// now 作为窗口锚点，保留当前周期和此前连续 MaxRetainTables-1 个周期。
func (m *tableManager) cleanupExpiredTables(db *gorm.DB, cfg ShardingConfig, now time.Time) error {
	if cfg.MaxRetainTables <= 0 {
		return nil
	}

	cutoff := cfg.shardTime(now)
	for i := 1; i < cfg.MaxRetainTables; i++ {
		cutoff = cfg.Strategy.Prev(cutoff)
	}
	cutoffStart, ok := cfg.Strategy.ParseSuffix(cfg.Strategy.Suffix(cutoff), cfg.Location)
	if !ok {
		return fmt.Errorf("gorm_sharding: strategy cannot parse retention cutoff")
	}
	tables, err := m.allExistingTables(db, cfg)
	if err != nil {
		return err
	}
	for _, table := range tables {
		periodStart, matches := shardPeriodStart(cfg, table)
		if !matches || !periodStart.Before(cutoffStart) {
			continue
		}
		// 表名来自 information_schema 且已严格校验固定格式，仍使用反引号转义防止标识符注入。
		if err := db.Session(&gorm.Session{NewDB: true}).Set(skipKey, true).
			Exec("DROP TABLE IF EXISTS " + quoteMySQLIdentifier(table)).Error; err != nil {
			return err
		}
		m.seen.Delete(table)
	}
	m.clearTableListCache(cfg)
	return nil
}

// allExistingTables 查询指定逻辑表前缀下的全部候选真实分表，供自动清理使用。
func (m *tableManager) allExistingTables(db *gorm.DB, cfg ShardingConfig) ([]string, error) {
	if db.Dialector.Name() != "mysql" {
		return nil, fmt.Errorf("gorm_sharding: only mysql table scan is supported")
	}

	var tables []string
	metadataDB := db.Session(&gorm.Session{NewDB: true})
	metadataDB.Error = nil
	err := metadataDB.Raw(
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE ? ESCAPE '\\\\'",
		tableNameLikePattern(cfg.TablePrefix),
	).Scan(&tables).Error
	return tables, err
}

// shardPeriodStart 将真实表名解析为当前策略所属周期的开始时间。
func shardPeriodStart(cfg ShardingConfig, table string) (time.Time, bool) {
	prefix := cfg.TablePrefix + "_"
	if !strings.HasPrefix(table, prefix) {
		return time.Time{}, false
	}
	suffix := strings.TrimPrefix(table, prefix)
	return cfg.Strategy.ParseSuffix(suffix, cfg.Location)
}

// quoteMySQLIdentifier 返回 MySQL 反引号转义后的标识符。
func quoteMySQLIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

// tables 返回无精确分表条件时需要扫描的最近分表列表。
func (m *tableManager) tables(db *gorm.DB, cfg ShardingConfig, now time.Time) []string {
	cacheKey := m.tableListCacheKey(cfg, now)
	if cached, ok := m.tableLists.Load(cacheKey); ok {
		return cloneStrings(cached.([]string))
	}

	tables, err := m.existingTables(db, cfg, now)
	if err == nil {
		m.tableLists.Store(cacheKey, cloneStrings(tables))
		return tables
	}

	// information_schema 不可用时退化为最近 N 个时间周期的候选表名。
	out := recentTableCandidates(cfg, now)
	m.tableLists.Store(cacheKey, cloneStrings(out))
	return out
}

// tableListCacheKey 生成最近表列表缓存键；当前分片变化时键也变化，避免跨小时/天/月继续使用旧列表。
func (m *tableManager) tableListCacheKey(cfg ShardingConfig, now time.Time) string {
	return fmt.Sprintf("%s|%d|%s", cfg.TablePrefix, cfg.MaxScanTables, cfg.tableName(now))
}

// clearTableListCache 清理指定逻辑表前缀的最近表列表缓存；自动建新分表后需要刷新扫描列表。
func (m *tableManager) clearTableListCache(cfg ShardingConfig) {
	prefix := cfg.TablePrefix + "|"
	m.tableLists.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), prefix) {
			m.tableLists.Delete(key)
		}
		return true
	})
}

// existingTables 返回最近 MaxScanTables 个时间周期内已经存在的真实分表。
func (m *tableManager) existingTables(db *gorm.DB, cfg ShardingConfig, now time.Time) ([]string, error) {
	if db.Dialector.Name() != "mysql" {
		return nil, fmt.Errorf("gorm_sharding: only mysql table scan is supported")
	}

	candidates := recentTableCandidates(cfg, now)
	if len(candidates) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(candidates))
	arguments := make([]interface{}, len(candidates))
	for index, table := range candidates {
		placeholders[index] = "?"
		arguments[index] = table
	}

	var found []string
	// 元数据查询不能复用当前业务 Statement：Raw(...).Scan(...) 会设置 Dest、Schema 和 SQL，
	// 从而破坏调用方随后执行的分表查询。NewDB Session 使用独立 Statement 保留同一连接配置。
	metadataDB := db.Session(&gorm.Session{NewDB: true})
	// 当前调用可能正在从 1146 恢复，不能让历史错误阻止 information_schema 查询。
	metadataDB.Error = nil
	err := metadataDB.Raw(
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ("+strings.Join(placeholders, ",")+")",
		arguments...,
	).Scan(&found).Error
	if err != nil {
		return nil, err
	}
	existing := make(map[string]struct{}, len(found))
	for _, table := range found {
		existing[table] = struct{}{}
		m.seen.Store(table, struct{}{})
	}
	tables := make([]string, 0, len(found))
	for _, table := range candidates {
		if _, ok := existing[table]; ok {
			tables = append(tables, table)
		}
	}
	return tables, nil
}

// recentTableCandidates 按当前周期向前生成最多 MaxScanTables 个不同的真实分表名。
func recentTableCandidates(cfg ShardingConfig, now time.Time) []string {
	tables := make([]string, 0, cfg.MaxScanTables)
	seen := make(map[string]struct{}, cfg.MaxScanTables)
	cursor := cfg.shardTime(now)
	for index := 0; index < cfg.MaxScanTables; index++ {
		table := cfg.tableName(cursor)
		if _, ok := seen[table]; !ok {
			tables = append(tables, table)
			seen[table] = struct{}{}
		}
		cursor = cfg.Strategy.Prev(cursor)
	}
	return tables
}

// tableNameLikePattern 转义逻辑表名里的 LIKE 通配符，并匹配其真实分表后缀。
func tableNameLikePattern(prefix string) string {
	escaped := strings.NewReplacer("\\", "\\\\", "_", "\\_", "%", "\\%").Replace(prefix)
	return escaped + "\\_%"
}

// autoMigrate 使用调用方传入的 DB 对已经存在的历史分表逐张执行 GORM AutoMigrate。
func (m *tableManager) autoMigrate(db *gorm.DB, model interface{}, cfg ShardingConfig) error {
	if db == nil {
		return fmt.Errorf("gorm_sharding: AutoMigrate database is nil")
	}
	tables, err := m.existingTables(db, cfg, time.Now())
	if err != nil {
		return err
	}
	for _, table := range tables {
		// 历史分表逐张迁移，保证新增字段同步到已经存在的真实表。
		if err := db.Session(&gorm.Session{NewDB: true}).Table(table).AutoMigrate(model); err != nil {
			return err
		}
	}
	return nil
}

// elemAt 返回指针、切片或数组中的第 i 个实际元素。
func elemAt(v reflect.Value, i int) reflect.Value {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		return v.Index(i)
	}
	return v
}

// cloneStrings 复制字符串切片，避免缓存值被调用方修改。
func cloneStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	return out
}
