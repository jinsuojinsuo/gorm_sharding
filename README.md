# gorm_sharding

`gorm_sharding` 是一个基于 GORM 的 MySQL 时间分表插件。业务代码继续使用普通 GORM 的 `Create`、`Find`、`Updates`、`Delete`、`Raw` 写法，插件负责根据时间字段路由到真实分表。

## 功能

1. 支持按年、月、周、日、小时分表。
2. 支持 `time.Time` 分表字段；`ShardingKey` 只支持数据库列名。
3. 插入时自动计算目标表，并在表不存在时使用 GORM `AutoMigrate` 自动创建；可按时间窗口自动清理过期分表。
4. 批量插入会按目标分表自动拆分。
5. 查询只根据 `WHERE` 中的分表字段精确路由；不包含可识别分表字段时最多扫描最近 `MaxScanTables` 个时间周期内存在的表。
6. 单模型、单逻辑表的跨分表读取统一由 MySQL 合并真实分表原始行后执行，保持单表查询结果与 GORM 回调语义一致。
7. Update/Delete 支持精确路由和最近 N 个周期扫描，并累加 `RowsAffected`；跨分表时不支持 `Limit`。
8. 支持单表 Raw SQL，通过 Vitess `sqlparser` 做 AST 表名改写。
9. 支持显式调用 `gorm_sharding.AutoMigrate(db, model)` 同步历史分表字段。
10. 不创建逻辑模板表，例如只创建 `user_2026080417`，不创建 `user`。

## 安装

```bash
go get github.com/jinsuojinsuo/gorm_sharding
```

## 基本用法

```go
package main

import (
	"time"

	"github.com/jinsuojinsuo/gorm_sharding"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type User struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name;type:varchar(64);not null;default:'';index"`
	Score     int       `gorm:"column:score;not null;default:0;index"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime;not null;index"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime;not null"`
}

func (User) TableName() string {
	return "user"
}

func main() {
	db, err := gorm.Open(mysql.Open("user:pass@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"))
	if err != nil {
		panic(err)
	}

	plugin := gorm_sharding.New()
	if err := plugin.Register(gorm_sharding.ShardingConfig{
	TablePrefix:     User{}.TableName(),
	ShardingKey:     "created_at",
	Strategy:        gorm_sharding.HourStrategy,
	Location:        time.Local,
	MaxScanTables:   3,
		AutoCreateTable: true,
		AutoMigrate:     true,
	}); err != nil {
		panic(err)
	}

	//必须要先 plugin.Register再db.Use
	if err := db.Use(plugin); err != nil {
		panic(err)
	}

	now := time.Now()
	if err := db.Create(&User{
		Name:      "alice",
		Score:     100,
		CreatedAt: now,
		UpdatedAt: now,
	}).Error; err != nil {
		panic(err)
	}
}
```

## 配置说明

```go
type ShardingConfig struct {
	// 逻辑表名和真实分表前缀，例如 user 会生成 user_202608。
	// 必须与业务模型的 TableName() 返回值或 GORM 默认表名一致。
	TablePrefix string

	// 分表字段的数据库列名，例如 created_at。
	ShardingKey string

	// 分表策略，决定表名后缀和最近表倒推粒度。
	Strategy ShardingStrategy

	// 分表使用的固定时区，例如 time.Local 或 time.UTC；必须显式配置。
	Location *time.Location

	// 无分表条件时最多扫描的最近连续时间周期数。
	MaxScanTables int

	// 自动保留的连续时间周期数；0 表示不自动删除历史分表。
	// 大于 0 时 Strategy 必须正确实现 ParseSuffix。
	MaxRetainTables int

	// 插入目标表不存在时是否自动建表。
	AutoCreateTable bool

	// 调用 gorm_sharding.AutoMigrate 时是否迁移该模型的历史分表。
	AutoMigrate bool
}
```

`TablePrefix` 由业务侧显式配置，插件不再接收模型参数。它必须与模型的 `TableName()` 返回值一致；未实现 `TableName()` 时，填写 GORM 默认命名规则解析出的表名。每个逻辑表只能注册一次。

`Location` 必须显式配置，插件会在所有表名计算、范围路由、最近周期扫描和自动清理时将时间转换到该时区。相同时间点使用 UTC 与 `Asia/Shanghai` 可能属于不同日期或小时分表，业务进程必须为同一逻辑表使用同一 `Location`。

### 自动清理历史分表

`MaxRetainTables` 按时间窗口而非实际已建表数量计算。设为 `N` 后，插件在创建当前周期的新分表成功时，保留当前周期及此前连续 `N-1` 个周期，并删除窗口外可识别的同前缀分表。例如按月分表且 `MaxRetainTables: 3`，在 2026 年 8 月创建新分表后保留 `202606`、`202607`、`202608`，删除更早月份的表；中间某月未建表也计入窗口。

自动清理支持内置和自定义策略。自定义 `ShardingStrategy` 必须正确实现 `ParseSuffix`，将真实分表后缀解析为该分表周期的开始时间；无法解析的同前缀表不会被删除。启用自动清理时，`MaxRetainTables` 必须大于或等于 `MaxScanTables`，否则 `Register` 会返回错误。外层事务内首次建表不会执行清理，避免 MySQL `DROP TABLE` 的隐式提交影响业务事务。

```go
type ShardingStrategy interface {
	Suffix(time.Time) string
	Prev(time.Time) time.Time
	ParseSuffix(suffix string, location *time.Location) (time.Time, bool)
}
```

## 分表策略

| 策略 | 后缀示例 | 表名示例 |
| --- | --- | --- |
| `YearStrategy` | `2026` | `user_2026` |
| `MonthStrategy` | `202608` | `user_202608` |
| `WeekStrategy` | `2026_w32` | `user_2026_w32` |
| `DayStrategy` | `20260804` | `user_20260804` |
| `HourStrategy` | `2026080417` | `user_2026080417` |

## CRUD 示例

### 插入

插件支持 GORM 常见 `Create` 插入方式。插入时会读取模型里的分表字段，计算目标真实表名；目标表不存在且 `AutoCreateTable` 为 `true` 时，会先自动建表。

```go
now := time.Now()

// 单条插入
err := db.Create(&User{
	Name:      "alice",
	Score:     100,
	CreatedAt: now,
	UpdatedAt: now,
}).Error

// 批量插入；不同分表的数据会自动按真实表拆分后逐表插入。
users := []User{
	{Name: "alice", CreatedAt: now, UpdatedAt: now},
	{Name: "bob", CreatedAt: now.Add(-time.Hour), UpdatedAt: now},
}
err = db.Create(&users).Error

// 显式批量插入；插件会保留 GORM 的批大小设置。
err = db.CreateInBatches(&users, 1000).Error

// 也支持通过 GORM Session 配置批大小。
err = db.Session(&gorm.Session{CreateBatchSize: 1000}).Create(&users).Error
```

批量插入规则：

1. 同一真实分表内的数据会继续使用 GORM 批量插入。
2. 跨真实分表的数据会先按目标表拆分，再分别执行插入。
3. `RowsAffected` 会累加所有真实分表的影响行数。
4. 分表字段必须能从每条记录中取到 `time.Time` 值，否则会返回错误。
5. `Create` 不能通过 `Select` 或 `Omit` 省略分表字段；`OnConflict` 也不能更新分表字段。两类操作都会返回错误，避免记录时间与物理分表不一致。

### 精确查询

```go
var users []User
err := db.Where("created_at = ? AND name = ?", createdAt, "alice").Find(&users).Error
```

### 范围查询

```go
var users []User
err := db.Where("created_at BETWEEN ? AND ?", start, end).Find(&users).Error
```

### 无分表字段查询

```go
var users []User
err := db.Where("name = ?", "alice").Find(&users).Error
```

该场景不会扫描全部历史表，只会扫描最近 `MaxScanTables` 个时间周期内存在的真实分表。中间周期未建表时，不会向更早周期补足扫描数量。

### 跨分表聚合

跨分表 `Count`、`COUNT(DISTINCT ...)`、`SUM`、`MIN`、`MAX`、`AVG`、`Group By`、`Having` 会由 MySQL 执行全局查询：插件使用 `UNION ALL` 合并各真实分表的原始行，再在外层保留原始 SQL 的 `SELECT`、聚合、分组、排序和分页。

跨分表普通 GORM 查询不支持子查询，包括相关子查询、`EXISTS` 和派生表；会返回 `gorm_sharding: subquery across shards is not supported`。单分表查询不受此限制。

跨分表查询不支持 `Preload`，会返回 `gorm_sharding: preload across shards is not supported`。关联模型可能也是分表模型，预加载条件通常无法提供关联表的分表字段，插件不会静默返回不完整关联数据。

对于单模型、单逻辑表且不包含 Join 的查询，`Find` 和 `Scan` 的结果可保持与单表 MySQL 查询一致，包括 `NULL` 语义、`COUNT(DISTINCT ...)`、加权 `AVG`、数据库排序规则和 `HAVING`。

```go
var total int64
err := db.Model(&User{}).
	Where("created_at BETWEEN ? AND ?", start, end).
	Count(&total).Error

// COUNT(DISTINCT ...) 会在全部命中分表的原始行合并后统一去重。
var distinctNames int64
err = db.Model(&User{}).
	Distinct("name").
	Where("created_at BETWEEN ? AND ?", start, end).
	Count(&distinctNames).Error

type ScoreStats struct {
	Total int64
	Min   int
	Max   int
	Avg   float64
}
var stats ScoreStats
err = db.Model(&User{}).
	Select("SUM(score) AS total, MIN(score) AS min, MAX(score) AS max, AVG(score) AS avg").
	Where("created_at BETWEEN ? AND ?", start, end).
	Scan(&stats).Error
```

分组聚合同样支持：

```go
type NameCount struct {
	Name  string
	Total int64
}
var groups []NameCount
err = db.Model(&User{}).
	Select("name, COUNT(*) AS total").
	Where("created_at BETWEEN ? AND ?", start, end).
	Group("name").
	Find(&groups).Error
```

聚合、`Group`、`Order`、`Select`、`Having` 中不要手写逻辑表限定名，例如 `user.score`；请使用 `score`。跨分表 Join 仍不支持。

### 支持的分表条件写法

插件会优先从 GORM `WHERE` 条件中解析分表字段，能识别以下常见写法：

```go
// 等值条件
db.Where("created_at = ?", createdAt)
db.Where("created_at = ? AND name = ?", createdAt, "alice")

// 带表名前缀或反引号
db.Where("users.created_at = ?", createdAt)
db.Where("`users`.`created_at` = ?", createdAt)

// BETWEEN 范围
db.Where("created_at BETWEEN ? AND ?", start, end)

// 半开区间
db.Where("created_at >= ? AND created_at < ?", start, end)
db.Where("created_at > ? AND created_at <= ?", start, end)

// 连续 Where 范围：插件会收集上下界并按交集精确路由
db.Where("created_at >= ?", start).
	Where("created_at < ?", end)

// IN 条件
db.Where("created_at IN (?, ?)", t1, t2)
db.Where("created_at IN ?", []time.Time{t1, t2})

// GORM clause 条件
db.Where(clause.Eq{Column: "created_at", Value: createdAt})
db.Where(clause.Gte{Column: "created_at", Value: start}).
	Where(clause.Lt{Column: "created_at", Value: end})
db.Where(clause.IN{Column: "created_at", Values: []interface{}{t1, t2}})
```

分表字段模型值仍必须是 `time.Time`。`Where`、`clause` 和 Raw SQL 的 `ShardingKey` 绑定参数支持 `time.Time`、RFC3339 字符串，以及 `2006-01-02`、`2006/01/02`、`2006-01-02 15:04:05`、`2006/01/02 15:04:05`、`2006-01-02T15:04:05`。无时区字符串按 `Location` 解释；RFC3339 字符串按自身偏移量解析后转换到 `Location`。

`LIKE` 仅支持连续日期前缀：`2026%`、`2026-08%`、`2026-08-04%`、`2026-08-04 10%`。其他通配形式、SQL 内联日期字面量和无法解释的分表字段表达式会返回错误。

如果条件里无法解析出分表字段，插件会退化为扫描最近 `MaxScanTables` 个时间周期内存在的真实分表；中间周期未建表时，不会向更早周期补足扫描数量。

所有 `ShardingKey` 条件都会先按 `Location` 归一化并校验，即使另一个条件已经足以精确路由。非法时间字符串、SQL 内联日期字面量和无法解释的分表字段表达式会直接返回错误。时间值合法但组合条件无法推导出完整有限范围时，插件扫描最近 `MaxScanTables` 个周期内存在的表；该降级策略不保证覆盖全部历史数据。

多个范围条件会按 `AND` 交集计算：下界取较晚时间，上界取较早时间；不会因范围分别写在多次 `Where` 调用中而退化为最近表扫描。

连续日期 `LIKE` 前缀同样会转换为半开时间范围，并与同一 `AND` 条件中的等值、`IN`、上下界取交集后路由。

当范围交集为空，例如 `created_at >= start AND created_at < end` 且 `start >= end` 时，插件直接返回空结果，`Update` 和 `Delete` 的 `RowsAffected` 为 `0`，不会扫描任何真实分表。

### 更新

只包含主键的 `Update`、`Updates`、`Delete` 会返回错误；分表间主键可能重复，单条写入必须同时包含可识别的分表字段条件。

多分表 `Create`、`Update`、`Delete` 会复用外层事务；没有外层事务时，即使配置了 `SkipDefaultTransaction`，插件也会创建内部事务，避免部分分表写入成功。

```go
res := db.Model(&User{}).
	Where("created_at = ? AND name = ?", createdAt, "alice").
	Updates(map[string]interface{}{
		"score":      120,
		"updated_at": time.Now(),
	})

// 批量实体更新会按每个模型实体的 CreatedAt 自动分组到对应分表。
res = db.Model(&[]User{
	{ID: 1, CreatedAt: firstCreatedAt},
	{ID: 2, CreatedAt: secondCreatedAt},
}).Updates(map[string]interface{}{"score": 120})
```

### 删除

```go
res := db.Where("created_at = ? AND name = ?", createdAt, "alice").Delete(&User{})

// 批量实体删除会按每个实体的 CreatedAt 自动分组到对应分表。
res = db.Delete(&[]User{
	{ID: 1, CreatedAt: firstCreatedAt},
	{ID: 2, CreatedAt: secondCreatedAt},
})
```

### Raw SQL

```go
var users []User
err := db.Raw("SELECT * FROM user WHERE created_at = ?", createdAt).Scan(&users).Error
```

Raw SQL 中的逻辑表名会通过 Vitess SQL AST 改写为真实分表名。

Raw `SELECT` 只支持路由到一张真实分表；`IN`、范围等条件命中多张表时会返回 `gorm_sharding: raw SQL across shards is not supported`。Raw `UPDATE`、`DELETE` 支持单模型、单逻辑表命中多张真实分表：插件会基于同一份 SQL AST 逐表执行并累加 `RowsAffected`。调用方已开启事务时复用外层事务；未开启事务时插件创建内部事务，除 `1146 Table doesn't exist` 外任一分表失败会回滚已执行的分表写入。`1146` 缺失分表按空表跳过并清理缓存。命中多张分表的 Raw `UPDATE`、`DELETE` 不支持 `LIMIT`，会返回 `gorm_sharding: limit across shards is not supported`。Raw `UPDATE ... JOIN`、多表 `DELETE`、派生表写入会返回 `gorm_sharding: raw multi-table write is not supported`。子查询可以访问普通非分表表，但不能引用已注册的逻辑分表；插件不会递归改写子查询中的逻辑表名。未包含可识别分表字段的 Raw 写操作只会扫描最近 `MaxScanTables` 个时间周期内存在的分表，**不保证覆盖全部历史分表**；中间周期未建表时不会向更早周期补足扫描数量。需要处理完整历史数据时，必须提供可识别的分表字段时间条件。Raw `INSERT` 到逻辑分表名会直接报错，请使用 `Create`，避免历史时间数据被写到错误分表。

不要通过连接串的 `multiStatements=true` 拼接多条跨分表 SQL。Raw `UPDATE`、`DELETE` 的多分表执行由插件管理；跨分表读取请使用 `Find`。

## 自动迁移

最近 `MaxScanTables` 个时间周期内存在的历史分表字段同步使用插件提供的迁移方法：

```go
if err := gorm_sharding.AutoMigrate(db, &User{}); err != nil {
	panic(err)
}
```

建议把它作为显式迁移动作执行，不建议每次业务进程启动都执行。GORM `AutoMigrate` 会对每张历史表查询字段、索引等元数据，表多时会产生较多 `information_schema` 查询。

插入新分表时，如果 `AutoCreateTable` 为 `true`，插件会使用最新 struct 自动创建新分表。

## 示例

仓库内提供了示例：

```bash
go run ./_example/gorm_sharding
```

示例默认连接：

```text
root:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local
```

运行前请确认本机 MySQL 账号、密码和数据库可用。

## 测试

运行需求验收测试：

```bash
go test ./... -run TestRequirement -count=1 -v
```

测试默认连接：

```text
root:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local
```

也可以用环境变量覆盖：

```bash
GORM_SHARDING_TEST_DSN='user:pass@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local' \
go test ./... -run TestRequirement -count=1 -v
```

测试只会创建和清理 `gs_req_` 前缀的测试表。

## 已知限制

1. 第一版只支持 MySQL。
2. 分表字段模型值必须是 `time.Time`，数据库字段建议使用 `DATETIME` 或 `TIMESTAMP`；查询绑定参数支持本文列出的日期字符串。
3. 不支持 int 时间戳、SQL 内联日期字面量和无法解释的 SQL 表达式计算分表字段。
4. 跨分表 Join 不支持。
5. 单模型、单逻辑表的跨分表 `Order`、`Offset`、`Limit`、`Distinct`、聚合、`Group By`、`Having` 已支持：由 MySQL 在合并后的原始行集上统一执行，以保持单表 SQL 语义。为减少每张表读取量，明细分页会在每张分表先执行相同排序并取 `offset + limit` 行；`FOR UPDATE`、`FOR SHARE` 等锁定查询不支持跨分表，会返回 `gorm_sharding: locking across shards is not supported`。
6. 跨分表查询不支持 Join、`Preload`，也不支持在 `Group`、`Order`、`Select`、`Having` 中手写逻辑表限定名，例如 `user.score`；应使用 `score`。
8. Raw `SELECT` 只支持单个真实分表；Raw `UPDATE`、`DELETE` 支持多分表循环执行。复杂 Join 仍不支持。
9. Raw SQL 的子查询可访问普通非分表表，但不能引用已注册的逻辑分表。跨分表普通 GORM 查询不支持任何子查询，包括相关子查询、`EXISTS` 和派生表；单分表 GORM 查询不受此限制。
10. 不接管 `db.AutoMigrate`，历史分表迁移请使用 `gorm_sharding.AutoMigrate`。
11. 事务内首次写入新分表时，插件会使用初始化时保存的非事务连接创建物理表，再回到原事务执行插入。因此业务 DML 可以正常回滚，但新建的空分表会保留。元数据读取仍继承当前调用的连接配置与 Context；为避免首次写入承受 DDL 延迟，建议提前预建下一周期分表。
12. 分表字段不可更新。GORM `Update`、`Updates`、`Save` 和 Raw `UPDATE` 修改该字段会返回错误；需要调整分表时间时，请由业务显式执行“插入新分表并删除旧分表”。
13. 普通 GORM 的 `Update`、`Updates`、`Delete` 若仅以主键定位记录且没有可识别的分表字段，会返回错误。分表间自增主键可能重复，单条写入应同时提供分表字段。
14. 不支持跨分表唯一索引。模型中的 `unique`、`uniqueIndex` 只在单张真实分表内生效；不同分表可以出现相同值。`OnConflict` 也只能检测目标真实分表，不能用于逻辑表级别的全局去重。需要全局唯一约束时，应由业务侧使用全局唯一 ID、独立索引表或其他全局约束方案。
15. 无分表字段的主键读取不保证唯一定位。分表间自增主键可能重复，`First`、`Take`、`Find` 等只按主键查询时可能命中多个真实分表中的任意一条记录。需要按主键精确读取时，应同时提供可识别的分表字段，或确保主键在所有分表间全局唯一。

## 性能说明

1. `MaxScanTables` 控制最多扫描多少张分表，不代表最多执行多少次元数据查询。
2. `gorm_sharding.AutoMigrate` 会对每张历史表执行 GORM 迁移检查，字段和索引越多，`information_schema` 查询越多。
3. 最近表列表按当前分片缓存，切到新分片时缓存 key 会变化。
4. 自动创建新分表后会清理最近表列表缓存，避免切表瞬间继续使用旧列表。
5. 执行 SQL 前不会逐表检查是否存在；如果执行后遇到 MySQL `1146 Table doesn't exist`，会清理缓存并按表不存在处理。

## 目录结构

```text
.
├── config.go
├── create.go
├── plugin.go
├── raw.go
├── route.go
├── state.go
├── strategy.go
├── table_manager.go
├── requirements_integration_test.go
├── gorm_sharding.md
└── _example/gorm_sharding
```
