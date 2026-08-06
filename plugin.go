package gorm_sharding

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

const (
	pluginName = "gorm_sharding"
	skipKey    = "gorm_sharding:skip"
)

// Plugin 是 GORM 分表插件主体，保存模型配置并接管 GORM 核心回调。
type Plugin struct {
	initMu      sync.Mutex
	initialized bool

	configs map[reflect.Type]ShardingConfig
	manager *tableManager

	// 保存 GORM 原始核心回调，分表插件完成路由后仍交回 GORM 执行，
	// 避免重写 GORM 的 SQL 构建、Hook、Scan、RowsAffected 等细节。
	createFn func(*gorm.DB)
	queryFn  func(*gorm.DB)
	updateFn func(*gorm.DB)
	deleteFn func(*gorm.DB)
	rowFn    func(*gorm.DB)
	rawFn    func(*gorm.DB)
}

// New 创建一个新的分表插件实例。
func New() *Plugin {
	return &Plugin{configs: make(map[reflect.Type]ShardingConfig)}
}

// Name 返回 GORM 插件名称。
func (p *Plugin) Name() string {
	return pluginName
}

// Register 注册模型和对应分表配置。
func (p *Plugin) Register(model interface{}, cfg ShardingConfig) error {
	p.initMu.Lock()
	defer p.initMu.Unlock()
	if p.initialized {
		return fmt.Errorf("gorm_sharding: register must happen before db.Use")
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	t := modelKey(model)
	if t == nil || t.Kind() != reflect.Struct {
		return fmt.Errorf("gorm_sharding: model must be struct or struct pointer")
	}
	p.configs[t] = cfg
	return nil
}

// Initialize 接入 GORM，替换 Create/Query/Update/Delete/Raw 的核心执行回调。
func (p *Plugin) Initialize(db *gorm.DB) error {
	p.initMu.Lock()
	defer p.initMu.Unlock()
	if p.initialized {
		return fmt.Errorf("gorm_sharding: plugin has already been initialized")
	}
	if err := p.resolveTablePrefixes(db); err != nil {
		return err
	}

	p.manager = newTableManager(db)

	// Replace 之前先取出原始回调；后续插件回调内部会在改完表名后调用它们。
	p.createFn = db.Callback().Create().Get("gorm:create")
	p.queryFn = db.Callback().Query().Get("gorm:query")
	p.updateFn = db.Callback().Update().Get("gorm:update")
	p.deleteFn = db.Callback().Delete().Get("gorm:delete")
	p.rowFn = db.Callback().Row().Get("gorm:row")
	p.rawFn = db.Callback().Raw().Get("gorm:raw")
	if err := db.Callback().Create().Replace("gorm:create", p.create); err != nil {
		return err
	}
	if err := db.Callback().Query().Replace("gorm:query", p.query); err != nil {
		return err
	}
	if err := db.Callback().Update().Replace("gorm:update", p.update); err != nil {
		return err
	}
	if err := db.Callback().Delete().Replace("gorm:delete", p.delete); err != nil {
		return err
	}
	if err := db.Callback().Row().Replace("gorm:row", p.row); err != nil {
		return err
	}
	if err := db.Callback().Raw().Replace("gorm:raw", p.raw); err != nil {
		return err
	}
	p.initialized = true
	return nil
}

// resolveTablePrefixes 使用 GORM schema 解析注册模型的逻辑表名，作为真实分表前缀。
func (p *Plugin) resolveTablePrefixes(db *gorm.DB) error {
	cache := &sync.Map{}
	prefixes := make(map[string]reflect.Type, len(p.configs))
	for modelType, cfg := range p.configs {
		model := reflect.New(modelType).Interface()
		parsed, err := schema.Parse(model, cache, db.Config.NamingStrategy)
		if err != nil {
			return err
		}
		// ShardingKey 统一使用数据库列名，避免 Go 字段名与 SQL 条件列名不一致导致路由退化。
		field := parsed.LookUpField(cfg.ShardingKey)
		if field == nil || field.DBName != cfg.ShardingKey {
			return fmt.Errorf("gorm_sharding: sharding key %s must be a database column name", cfg.ShardingKey)
		}
		if previous, exists := prefixes[parsed.Table]; exists {
			return fmt.Errorf("gorm_sharding: models %s and %s use the same logical table %s", previous, modelType, parsed.Table)
		}
		prefixes[parsed.Table] = modelType
		cfg.tablePrefix = parsed.Table
		p.configs[modelType] = cfg
	}
	return nil
}

// AutoMigrate 迁移已注册模型最近 MaxScanTables 张历史分表，不创建逻辑模板表。
func (p *Plugin) AutoMigrate(db *gorm.DB, models ...interface{}) error {
	for _, model := range models {
		cfg, ok := p.configs[modelKey(model)]
		if !ok || !cfg.AutoMigrate {
			continue
		}
		// 需求要求无模板表，所以这里不迁移逻辑表，只迁移已经存在的历史分表。
		if err := p.manager.autoMigrate(db, model, cfg); err != nil {
			return err
		}
	}
	return nil
}

// configFor 根据当前 GORM Statement 找到模型对应的分表配置。
func (p *Plugin) configFor(db *gorm.DB) (ShardingConfig, bool) {
	if skipped, ok := db.Get(skipKey); ok && skipped == true {
		return ShardingConfig{}, false
	}
	if db.Statement == nil {
		return ShardingConfig{}, false
	}
	if db.Statement.Schema != nil {
		cfg, ok := p.configs[db.Statement.Schema.ModelType]
		return cfg, ok
	}
	if db.Statement.Model != nil {
		cfg, ok := p.configs[modelKey(db.Statement.Model)]
		return cfg, ok
	}
	if db.Statement.Dest != nil {
		cfg, ok := p.configs[modelKey(db.Statement.Dest)]
		return cfg, ok
	}
	return ShardingConfig{}, false
}

// create 处理插入路由、自动建表和批量插入分组。
func (p *Plugin) create(db *gorm.DB) {
	cfg, ok := p.configFor(db)
	if !ok {
		p.createFn(db)
		return
	}
	if !createIncludesShardingKey(db, cfg) {
		db.AddError(fmt.Errorf("gorm_sharding: create must include sharding key %s", cfg.ShardingKey))
		return
	}
	if createUpdatesShardingKey(db, cfg) {
		db.AddError(fmt.Errorf("gorm_sharding: updating sharding key %s is not supported", cfg.ShardingKey))
		return
	}

	groups, err := p.groupCreateValues(db, cfg)
	if err != nil {
		db.AddError(err)
		return
	}
	if len(groups) == 1 {
		for table := range groups {
			// 单分表插入不预查元数据，直接执行；只有 MySQL 返回 1146 后才建表并重试。
			setStatementTable(db, table)
			p.createFn(db)
			if isMissingTableError(db.Error) && cfg.AutoCreateTable {
				// 表在存在性缓存命中后被外部删除时，清理缓存、重建一次并重试本次写入。
				p.manager.invalidate(cfg, table)
				if err := p.manager.ensure(db, db.Statement.Model, cfg, table); err != nil {
					db.AddError(err)
					return
				}
				db.Error = nil
				db.Statement.SQL.Reset()
				db.Statement.Vars = nil
				p.createFn(db)
			}
			return
		}
	}

	rows, err := executeMultiShardWrite(db, func(writeDB *gorm.DB) (int64, error) {
		var rows int64
		for table, group := range groups {
			// 批量数据可能分散到多张表，必须拆成多次 Create，并累加影响行数。
			// skipKey 防止这些内部 Create 再次进入分表回调造成递归。
			res, err := p.createShardValues(writeDB, cfg, table, group.values.Interface())
			if err != nil {
				return 0, err
			}
			if err := res.Error; err != nil {
				return 0, err
			}
			// 分组时 []T 会被拆到临时切片中；插入成功后回写 GORM 生成的主键等字段。
			group.copyGeneratedValues()
			rows += res.RowsAffected
		}
		return rows, nil
	})
	if err != nil {
		db.AddError(err)
		return
	}
	db.RowsAffected = rows
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = rows
	}
}

// createShardValues 执行单个真实分表的内部插入，并在缓存表被外部删除时恢复一次。
func (p *Plugin) createShardValues(db *gorm.DB, cfg ShardingConfig, table string, values interface{}) (*gorm.DB, error) {
	create := func() *gorm.DB {
		tx := db.Session(&gorm.Session{NewDB: true, SkipHooks: true}).Table(table)
		copyCreateState(tx, db)
		tx = tx.Set(skipKey, true)
		return createShardValues(tx, values)
	}

	res := create()
	if !isMissingTableError(res.Error) || !cfg.AutoCreateTable {
		return res, nil
	}
	p.manager.invalidate(cfg, table)
	if err := p.manager.ensure(db, db.Statement.Model, cfg, table); err != nil {
		return nil, err
	}
	return create(), nil
}

// createShardValues 执行单个真实分表的插入。
// 当当前 GORM 会话配置了 CreateBatchSize 时，继续走 GORM 的批量插入接口，
// 避免分表拆分后的内部 Create 丢失批量大小；未配置时保持一次同表批量插入。
func createShardValues(db *gorm.DB, values interface{}) *gorm.DB {
	if db.CreateBatchSize > 0 {
		return db.CreateInBatches(values, db.CreateBatchSize)
	}
	return db.Create(values)
}

// query 处理查询路由，单表交给 GORM，跨表逐表查询并合并结果。
func (p *Plugin) query(db *gorm.DB) {
	if db.Statement.SQL.Len() > 0 {
		// db.Raw(...).Scan(...) 已经提前写好了 SQL，它走 Query 回调而不是 RawExec 回调。
		// 这里先尝试 AST 改写逻辑表名，再交给 GORM 原始 query 回调执行和扫描结果。
		sql, ok, err := p.rewriteRawSQL(db)
		if err != nil {
			db.AddError(err)
			return
		}
		if ok {
			db.Statement.SQL.Reset()
			db.Statement.SQL.WriteString(sql)
		}
		p.queryFn(db)
		return
	}

	cfg, ok := p.configFor(db)
	if !ok {
		p.queryFn(db)
		return
	}
	tables := p.routeReadTables(db, cfg)
	if len(tables) == 0 {
		p.executeEmptyRead(db, p.queryFn)
		return
	}
	if len(tables) <= 1 {
		if len(tables) == 1 {
			// 精确命中一张表时保持普通 GORM 查询路径，返回值和单表体验一致。
			setStatementTable(db, tables[0])
		}
		p.queryFn(db)
		if len(tables) == 1 && p.handleMissingTable(db, cfg, tables[0]) {
			return
		}
		return
	}
	if len(db.Statement.Joins) > 0 {
		db.AddError(fmt.Errorf("gorm_sharding: join query is not supported"))
		return
	}
	if len(db.Statement.Preloads) > 0 {
		db.AddError(fmt.Errorf("gorm_sharding: preload across shards is not supported"))
		return
	}
	if hasCrossShardLocking(db) {
		db.AddError(fmt.Errorf("gorm_sharding: locking across shards is not supported"))
		return
	}
	// 所有跨分表读取都由 MySQL 在合并原始行后统一执行。即使查询没有显式
	// Order 或聚合，也不能依赖 Go 追加切片来模拟数据库的结果集和 Row 回调语义。
	if err := p.executeCombinedQuery(db, cfg, tables); err != nil {
		db.AddError(err)
	}
}

// update 处理更新请求，按分表条件决定单表更新或多表扫描更新。
func (p *Plugin) update(db *gorm.DB) {
	p.execUpdateAcrossTables(db)
}

// delete 处理删除请求，按分表条件决定单表删除或多表扫描删除。
func (p *Plugin) delete(db *gorm.DB) {
	p.execDeleteAcrossTables(db)
}

// execUpdateAcrossTables 执行 Update 的单表路由或多表扫描逻辑。
func (p *Plugin) execUpdateAcrossTables(db *gorm.DB) {
	cfg, ok := p.configFor(db)
	if !ok {
		p.updateFn(db)
		return
	}
	if updatesShardingKey(db, cfg) {
		db.AddError(fmt.Errorf("gorm_sharding: updating sharding key %s is not supported", cfg.ShardingKey))
		return
	}
	if primaryKeyWriteWithoutShardingKey(db, cfg) {
		db.AddError(fmt.Errorf("gorm_sharding: primary key update requires sharding key %s", cfg.ShardingKey))
		return
	}
	groups, grouped, err := p.groupUpdateValues(db, cfg)
	if err != nil {
		db.AddError(err)
		return
	}
	if grouped {
		p.execUpdateGroups(db, cfg, groups)
		return
	}
	tables := p.routeWriteTables(db, cfg)
	if len(tables) == 0 {
		setEmptyWriteResult(db)
		return
	}
	if len(tables) <= 1 {
		if len(tables) == 1 {
			setStatementTable(db, tables[0])
		}
		p.updateFn(db)
		if len(tables) == 1 && p.handleMissingTable(db, cfg, tables[0]) {
			return
		}
		return
	}
	if err := crossShardWriteLimitError(db); err != nil {
		db.AddError(err)
		return
	}
	rows, err := executeMultiShardWrite(db, func(writeDB *gorm.DB) (int64, error) {
		var rows int64
		for _, table := range tables {
			// Update/Delete 没有分表条件时扫描最近 N 张表，每张表独立执行并累加影响行数。
			tx := writeDB.Session(&gorm.Session{NewDB: true}).Table(table)
			copyWriteState(tx, writeDB)
			tx = tx.Set(skipKey, true)
			tx = tx.Model(writeDB.Statement.Model).Updates(writeDB.Statement.Dest)
			if tx.Error != nil {
				if isMissingTableError(tx.Error) {
					p.manager.invalidate(cfg, table)
					continue
				}
				return 0, tx.Error
			}
			rows += tx.RowsAffected
		}
		return rows, nil
	})
	if err != nil {
		db.AddError(err)
		return
	}
	db.RowsAffected = rows
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = rows
	}
}

// execUpdateGroups 按真实分表执行批量实体更新，避免跨分表切片按首元素错误路由。
func (p *Plugin) execUpdateGroups(db *gorm.DB, cfg ShardingConfig, groups map[string]reflect.Value) {
	if len(groups) == 1 {
		for table := range groups {
			setStatementTable(db, table)
			p.updateFn(db)
			if p.handleMissingTable(db, cfg, table) {
				return
			}
			return
		}
	}
	if err := crossShardWriteLimitError(db); err != nil {
		db.AddError(err)
		return
	}

	rows, err := executeMultiShardWrite(db, func(writeDB *gorm.DB) (int64, error) {
		var rows int64
		for table, values := range groups {
			tx := p.updateShardValues(writeDB, table, values.Interface())
			if tx.Error != nil {
				if isMissingTableError(tx.Error) {
					p.manager.invalidate(cfg, table)
					continue
				}
				return 0, tx.Error
			}
			rows += tx.RowsAffected
		}
		return rows, nil
	})
	if err != nil {
		db.AddError(err)
		return
	}
	db.RowsAffected = rows
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = rows
	}
}

// execDeleteAcrossTables 执行 Delete 的单表路由或多表扫描逻辑。
func (p *Plugin) execDeleteAcrossTables(db *gorm.DB) {
	cfg, ok := p.configFor(db)
	if !ok {
		p.deleteFn(db)
		return
	}
	if primaryKeyWriteWithoutShardingKey(db, cfg) {
		db.AddError(fmt.Errorf("gorm_sharding: primary key delete requires sharding key %s", cfg.ShardingKey))
		return
	}
	groups, grouped, err := p.groupDeleteValues(db, cfg)
	if err != nil {
		db.AddError(err)
		return
	}
	if grouped {
		p.execDeleteGroups(db, cfg, groups)
		return
	}
	tables := p.routeWriteTables(db, cfg)
	if len(tables) == 0 {
		setEmptyWriteResult(db)
		return
	}
	if len(tables) <= 1 {
		if len(tables) == 1 {
			setStatementTable(db, tables[0])
		}
		p.deleteFn(db)
		if len(tables) == 1 && p.handleMissingTable(db, cfg, tables[0]) {
			return
		}
		return
	}
	if err := crossShardWriteLimitError(db); err != nil {
		db.AddError(err)
		return
	}
	rows, err := executeMultiShardWrite(db, func(writeDB *gorm.DB) (int64, error) {
		var rows int64
		for _, table := range tables {
			// Delete 多表扫描也必须走 GORM 公共 API，让 GORM 自己完成 Statement 初始化。
			tx := writeDB.Session(&gorm.Session{NewDB: true}).Table(table)
			copyWriteState(tx, writeDB)
			tx = tx.Set(skipKey, true)
			tx = tx.Delete(writeDB.Statement.Dest)
			if tx.Error != nil {
				if isMissingTableError(tx.Error) {
					p.manager.invalidate(cfg, table)
					continue
				}
				return 0, tx.Error
			}
			rows += tx.RowsAffected
		}
		return rows, nil
	})
	if err != nil {
		db.AddError(err)
		return
	}
	db.RowsAffected = rows
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = rows
	}
}

// execDeleteGroups 按真实分表执行批量实体删除，避免跨分表切片按首元素错误路由。
func (p *Plugin) execDeleteGroups(db *gorm.DB, cfg ShardingConfig, groups map[string]reflect.Value) {
	if len(groups) == 1 {
		for table := range groups {
			setStatementTable(db, table)
			p.deleteFn(db)
			if p.handleMissingTable(db, cfg, table) {
				return
			}
			return
		}
	}
	if err := crossShardWriteLimitError(db); err != nil {
		db.AddError(err)
		return
	}

	rows, err := executeMultiShardWrite(db, func(writeDB *gorm.DB) (int64, error) {
		var rows int64
		for table, values := range groups {
			tx := p.deleteShardValues(writeDB, table, values.Interface())
			if tx.Error != nil {
				if isMissingTableError(tx.Error) {
					p.manager.invalidate(cfg, table)
					continue
				}
				return 0, tx.Error
			}
			rows += tx.RowsAffected
		}
		return rows, nil
	})
	if err != nil {
		db.AddError(err)
		return
	}
	db.RowsAffected = rows
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = rows
	}
}

// raw 处理 GORM Raw/Exec SQL 的表名 AST 改写并执行原始 Raw 回调。
func (p *Plugin) raw(db *gorm.DB) {
	if handled, err := p.executeRawWriteAcrossShards(db); handled {
		if err != nil {
			db.AddError(err)
		}
		return
	}
	sql, ok, err := p.rewriteRawSQL(db)
	if err != nil {
		db.AddError(err)
		return
	}
	if ok {
		db.Statement.SQL.Reset()
		db.Statement.SQL.WriteString(sql)
	}
	p.rawFn(db)
}

// row 处理 db.Raw(...).Scan/Rows/Row 这类走 Row 回调的 SQL 改写。
func (p *Plugin) row(db *gorm.DB) {
	if db.Statement.SQL.Len() > 0 {
		sql, ok, err := p.rewriteRawSQL(db)
		if err != nil {
			db.AddError(err)
			return
		}
		if ok {
			db.Statement.SQL.Reset()
			db.Statement.SQL.WriteString(sql)
		}
		p.rowFn(db)
		return
	}

	cfg, ok := p.configFor(db)
	if !ok {
		p.rowFn(db)
		return
	}
	tables := p.routeReadTables(db, cfg)
	if len(tables) == 0 {
		if err := p.executeEmptyRowRead(db, cfg); err != nil {
			db.AddError(err)
		}
		return
	}
	if len(tables) <= 1 {
		if len(tables) == 1 {
			setStatementTable(db, tables[0])
		}
		p.rowFn(db)
		return
	}
	if len(db.Statement.Joins) > 0 {
		db.AddError(fmt.Errorf("gorm_sharding: join query is not supported"))
		return
	}
	if hasCrossShardLocking(db) {
		db.AddError(fmt.Errorf("gorm_sharding: locking across shards is not supported"))
		return
	}
	// Row 回调必须提供一个真实的 *sql.Rows。跨表时使用组合 SQL，
	// 才能让 Scan、Rows、Row 与单表 GORM 回调保持相同的返回语义。
	if err := p.executeCombinedRow(db, cfg, tables); err != nil {
		db.AddError(err)
		return
	}
}

// executeEmptyRead 使用恒为空的结果集完成 Query 或 Row 回调，保持 Find、Scan、Row 的空结果语义。
func (p *Plugin) executeEmptyRead(db *gorm.DB, execute func(*gorm.DB)) {
	db.Statement.SQL.Reset()
	db.Statement.SQL.WriteString("SELECT 1 AS gorm_sharding_empty FROM DUAL WHERE 1 = 0")
	db.Statement.Vars = nil
	execute(db)
}

// executeEmptyRowRead 在矛盾范围的 Rows 调用中保留原查询列结构。
func (p *Plugin) executeEmptyRowRead(db *gorm.DB, cfg ShardingConfig) error {
	value, ok := db.Get("rows")
	isRows, _ := value.(bool)
	if !ok || !isRows {
		// Row() 只需要返回 sql.ErrNoRows，不会暴露结果集列元数据，使用恒为空查询即可。
		p.executeEmptyRead(db, p.rowFn)
		return db.Error
	}

	// Rows() 会把 *sql.Rows 交给调用方，Columns() 必须与原始 SELECT 一致。
	// 因此不能使用虚拟 SELECT，而要在一张确认存在的真实分表执行原 WHERE 条件。
	tables, err := p.manager.existingTables(db, cfg)
	if err != nil {
		return fmt.Errorf("gorm_sharding: find existing shard for empty Rows: %w", err)
	}
	if len(tables) == 0 {
		return fmt.Errorf("gorm_sharding: empty Rows requires an existing shard")
	}

	setStatementTable(db, tables[0])
	p.rowFn(db)
	if !isMissingTableError(db.Error) {
		return db.Error
	}

	// 元数据查询与执行之间表可能被外部删除。清理缓存后只重试一次，仍不存在则明确报错。
	retryTables := p.manager.existingAfterMissing(db, cfg, tables)
	if len(retryTables) == 0 {
		return fmt.Errorf("gorm_sharding: empty Rows requires an existing shard")
	}
	db.Error = nil
	db.Statement.SQL.Reset()
	db.Statement.Vars = nil
	// GORM 的 Row 回调在第一次执行后会删除 rows 标记；重试前必须恢复，
	// 否则会错误地调用 QueryRowContext 并丢失 *sql.Rows 结果。
	db.Statement.Settings.Store("rows", true)
	setStatementTable(db, retryTables[0])
	p.rowFn(db)
	return db.Error
}

// setEmptyWriteResult 在矛盾时间范围下跳过 Update/Delete，并返回零影响行数。
func setEmptyWriteResult(db *gorm.DB) {
	db.RowsAffected = 0
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = 0
	}
}

// setStatementTable 把当前 GORM Statement 的逻辑表切换成真实分表。
func setStatementTable(db *gorm.DB, table string) {
	// GORM 构建 SQL 时会同时参考 Table 和 TableExpr，两者都需要切到真实分表。
	db.Statement.Table = table
	db.Statement.TableExpr = &clause.Expr{SQL: db.Statement.Quote(table)}
}

// hasCrossShardLocking 判断查询是否包含 GORM 的 FOR UPDATE、FOR SHARE 等锁定子句。
func hasCrossShardLocking(db *gorm.DB) bool {
	_, ok := db.Statement.Clauses["FOR"]
	return ok
}

// crossShardWriteLimitError 拒绝跨分表 Update/Delete 的 LIMIT，避免每张分表各自应用限制。
func crossShardWriteLimitError(db *gorm.DB) error {
	if _, _, ok := statementLimit(db); ok {
		return fmt.Errorf("gorm_sharding: limit across shards is not supported")
	}
	return nil
}

// executeMultiShardWrite 在没有外层事务时为多分表 DML 创建内部事务。
// GORM 默认会在回调外开启事务；但 SkipDefaultTransaction 为 true 时必须在此补齐，
// 否则某个分表失败会留下前面已成功的分表写入。
func executeMultiShardWrite(db *gorm.DB, execute func(*gorm.DB) (int64, error)) (int64, error) {
	if hasActiveTransaction(db) {
		return execute(db)
	}

	var rows int64
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		rows, err = execute(tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// routeReadTables 只依据 WHERE 条件计算读取操作的真实分表，不能使用查询结果对象的字段值。
func (p *Plugin) routeReadTables(db *gorm.DB, cfg ShardingConfig) []string {
	// Find 的结果对象可能预先带有字段值，但 GORM 不会把这些值变成 WHERE 条件。
	// 读取路由必须与 GORM 实际生成的筛选条件一致，避免错误地漏扫分表。
	if where, ok := db.Statement.Clauses["WHERE"].Expression.(clause.Where); ok {
		if tables, ok := tablesFromExprs(where.Exprs, cfg, cfg.ShardingKey); ok {
			return tables
		}
	}
	return p.manager.tables(db, cfg, time.Now())
}

// routeWriteTables 根据写入模型或 WHERE 条件计算 Update、Delete 的真实分表列表。
func (p *Plugin) routeWriteTables(db *gorm.DB, cfg ShardingConfig) []string {
	// 按实体更新或删除时，模型字段是业务显式提供的分表定位信息，应优先使用。
	if t, ok := timeFromReflect(db.Statement.ReflectValue, db.Statement.Schema, cfg.ShardingKey); ok {
		return []string{cfg.tableName(t)}
	}
	return p.routeReadTables(db, cfg)
}

// handleMissingTable 清理被外部删除的表缓存，并把本次操作按目标表不存在处理。
func (p *Plugin) handleMissingTable(db *gorm.DB, cfg ShardingConfig, table string) bool {
	if !isMissingTableError(db.Error) {
		return false
	}
	p.manager.invalidate(cfg, table)
	db.Error = nil
	db.RowsAffected = 0
	if db.Statement.Result != nil {
		db.Statement.Result.RowsAffected = 0
	}
	return true
}
