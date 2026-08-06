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
	// seen 缓存已经确认存在的真实分表，避免循环内重复 HasTable。
	seen sync.Map

	// tableLists 缓存“最近 N 张表”的查询结果；缓存键包含当前分片，跨分片时间后会自动失效。
	tableLists sync.Map

	// createGroup 合并同一真实分表的并发建表请求，避免切表瞬间重复 AutoMigrate。
	createGroup singleflight.Group
}

// newTableManager 创建表管理器实例。
func newTableManager() *tableManager {
	return &tableManager{}
}

// exists 判断真实分表是否存在；已经确认存在的表会缓存，避免循环扫描时反复查 information_schema。
func (m *tableManager) exists(db *gorm.DB, table string) bool {
	if _, ok := m.seen.Load(table); ok {
		return true
	}
	if !db.Migrator().HasTable(table) {
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
	_, err, _ := m.createGroup.Do(table, func() (interface{}, error) {
		if m.exists(db, table) {
			return nil, nil
		}
		// 不手写 CREATE TABLE，直接让 GORM AutoMigrate 根据 struct 和 tag 创建真实分表。
		if err := db.Session(&gorm.Session{NewDB: true}).Table(table).AutoMigrate(model); err != nil {
			return nil, err
		}
		m.seen.Store(table, struct{}{})
		m.clearTableListCache(cfg)
		return nil, nil
	})
	return err
}

// tables 返回无精确分表条件时需要扫描的最近分表列表。
func (m *tableManager) tables(db *gorm.DB, cfg ShardingConfig, now time.Time) []string {
	cacheKey := m.tableListCacheKey(cfg, now)
	if cached, ok := m.tableLists.Load(cacheKey); ok {
		return cloneStrings(cached.([]string))
	}

	tables, err := m.existingTables(db, cfg)
	if err == nil && len(tables) > 0 {
		if len(tables) > cfg.MaxScanTables {
			tables = tables[:cfg.MaxScanTables]
		}
		m.tableLists.Store(cacheKey, cloneStrings(tables))
		return tables
	}

	// information_schema 不可用时退化为按当前时间倒推，仍然受 MaxScanTables 限制。
	out := make([]string, 0, cfg.MaxScanTables)
	cursor := now
	for i := 0; i < cfg.MaxScanTables; i++ {
		out = append(out, cfg.tableName(cursor))
		cursor = cfg.Strategy.Prev(cursor)
	}
	m.tableLists.Store(cacheKey, cloneStrings(out))
	return out
}

// tableListCacheKey 生成最近表列表缓存键；当前分片变化时键也变化，避免跨小时/天/月继续使用旧列表。
func (m *tableManager) tableListCacheKey(cfg ShardingConfig, now time.Time) string {
	return fmt.Sprintf("%s|%d|%s", cfg.tablePrefix, cfg.MaxScanTables, cfg.tableName(now))
}

// clearTableListCache 清理指定逻辑表前缀的最近表列表缓存；自动建新分表后需要刷新扫描列表。
func (m *tableManager) clearTableListCache(cfg ShardingConfig) {
	prefix := cfg.tablePrefix + "|"
	m.tableLists.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), prefix) {
			m.tableLists.Delete(key)
		}
		return true
	})
}

// existingTables 从 MySQL information_schema 查询已经存在的真实分表。
func (m *tableManager) existingTables(db *gorm.DB, cfg ShardingConfig) ([]string, error) {
	if db.Dialector.Name() != "mysql" {
		return nil, fmt.Errorf("gorm_sharding: only mysql table scan is supported")
	}

	var tables []string
	// 按表名倒序取最近分表；当前策略的表名后缀都按时间递增，倒序就是从新到旧。
	err := db.Raw(
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE ? ESCAPE '\\\\' ORDER BY TABLE_NAME DESC LIMIT ?",
		tableNameLikePattern(cfg.tablePrefix), cfg.MaxScanTables,
	).Scan(&tables).Error
	if err != nil {
		return tables, err
	}
	for _, table := range tables {
		// existingTables 已经从 information_schema 拿到真实存在的表名，顺手写入缓存供后续扫描使用。
		m.seen.Store(table, struct{}{})
	}
	return tables, err
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
	tables, err := m.existingTables(db, cfg)
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
