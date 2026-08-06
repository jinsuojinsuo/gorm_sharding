# gorm_sharding

`gorm_sharding` 是一个基于 GORM 的 MySQL 时间分表插件。业务代码继续使用普通 GORM 的 `Create`、`Find`、`Updates`、`Delete`、`Raw` 写法，插件负责根据时间字段路由到真实分表。

## 功能

1. 支持按年、月、周、日、小时分表。
2. 支持 `time.Time` 分表字段，字段名可使用 Go 字段名或数据库列名。
3. 插入时自动计算目标表，并在表不存在时使用 GORM `AutoMigrate` 自动创建。
4. 批量插入会按目标分表自动拆分。
5. 查询包含分表字段时精确路由；不包含分表字段时最多扫描最近 `MaxScanTables` 张表。
6. 单模型、单逻辑表的跨分表读取统一由 MySQL 合并真实分表原始行后执行，保持单表查询结果与 GORM 回调语义一致。
7. Update/Delete 支持精确路由和最近 N 表扫描，并累加 `RowsAffected`。
8. 支持单表 Raw SQL，通过 Vitess `sqlparser` 做 AST 表名改写。
9. 支持显式调用 `plugin.AutoMigrate(db, model)` 同步历史分表字段。
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
	if err := plugin.Register(User{}, gorm_sharding.ShardingConfig{
		ShardingKey:     "created_at",
		Strategy:        gorm_sharding.HourStrategy,
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
	// 分表字段的数据库列名，例如 created_at。
	ShardingKey string

	// 分表策略，决定表名后缀和最近表倒推粒度。
	Strategy ShardingStrategy

	// 无分表条件时最多扫描的最近分表数量。
	MaxScanTables int

	// 插入目标表不存在时是否自动建表。
	AutoCreateTable bool

	// 调用 plugin.AutoMigrate 时是否迁移该模型的历史分表。
	AutoMigrate bool
}
```

真实分表前缀来自 GORM 逻辑表名。模型实现 `TableName()` 时使用该返回值；未实现时使用 GORM 默认命名规则。

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

该场景不会扫描全部历史表，只会扫描最近 `MaxScanTables` 张真实分表。

### 跨分表聚合

跨分表 `Count`、`COUNT(DISTINCT ...)`、`SUM`、`MIN`、`MAX`、`AVG`、`Group By`、`Having` 会由 MySQL 执行全局查询：插件使用 `UNION ALL` 合并各真实分表的原始行，再在外层保留原始 SQL 的 `SELECT`、聚合、分组、排序和分页。

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

如果条件里无法解析出分表字段，插件会退化为扫描最近 `MaxScanTables` 张真实分表。

多个范围条件会按 `AND` 交集计算：下界取较晚时间，上界取较早时间；不会因范围分别写在多次 `Where` 调用中而退化为最近表扫描。

### 更新

```go
res := db.Model(&User{}).
	Where("created_at = ? AND name = ?", createdAt, "alice").
	Updates(map[string]interface{}{
		"score":      120,
		"updated_at": time.Now(),
	})
```

### 删除

```go
res := db.Where("created_at = ? AND name = ?", createdAt, "alice").Delete(&User{})
```

### Raw SQL

```go
var users []User
err := db.Raw("SELECT * FROM user WHERE created_at = ?", createdAt).Scan(&users).Error
```

Raw SQL 中的逻辑表名会通过 Vitess SQL AST 改写为真实分表名。

Raw `SELECT` 只支持路由到一张真实分表；`IN`、范围等条件命中多张表时会返回 `gorm_sharding: raw SQL across shards is not supported`。Raw `UPDATE`、`DELETE` 支持单模型、单逻辑表命中多张真实分表：插件会基于同一份 SQL AST 逐表执行并累加 `RowsAffected`。调用方已开启事务时复用外层事务；未开启事务时插件创建内部事务，除 `1146 Table doesn't exist` 外任一分表失败会回滚已执行的分表写入。`1146` 缺失分表按空表跳过并清理缓存。Raw `UPDATE ... JOIN`、多表 `DELETE`、派生表写入会返回 `gorm_sharding: raw multi-table write is not supported`。未包含可识别分表字段的 Raw 写操作会扫描最近 `MaxScanTables` 张分表。Raw `INSERT` 到逻辑分表名会直接报错，请使用 `Create`，避免历史时间数据被写到错误分表。

不要通过连接串的 `multiStatements=true` 拼接多条跨分表 SQL。Raw `UPDATE`、`DELETE` 的多分表执行由插件管理；跨分表读取请使用 `Find`。

## 自动迁移

最近 `MaxScanTables` 张历史分表的字段同步使用插件提供的迁移方法：

```go
if err := plugin.AutoMigrate(db, &User{}); err != nil {
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
2. 分表字段只支持 `time.Time`，数据库字段建议使用 `DATETIME` 或 `TIMESTAMP`。
3. 不支持 int 时间戳、字符串日期、SQL 表达式计算分表字段。
4. 跨分表 Join 不支持。
5. 单模型、单逻辑表的跨分表 `Order`、`Offset`、`Limit`、`Distinct`、聚合、`Group By`、`Having` 已支持：由 MySQL 在合并后的原始行集上统一执行，以保持单表 SQL 语义。为减少每张表读取量，明细分页会在每张分表先执行相同排序并取 `offset + limit` 行。
6. 跨分表查询不支持 Join，也不支持在 `Group`、`Order`、`Select`、`Having` 中手写逻辑表限定名，例如 `user.score`；应使用 `score`。
8. Raw `SELECT` 只支持单个真实分表；Raw `UPDATE`、`DELETE` 支持多分表循环执行。复杂 Join 仍不支持。
9. 不接管 `db.AutoMigrate`，历史分表迁移请使用 `plugin.AutoMigrate`。
10. 不要在事务内依赖 `AutoCreateTable` 创建首次分表。MySQL DDL 不能与业务 DML 保持同一提交/回滚边界，插件会在初始化 DB 连接上创建表；请在事务开始前预建目标分表。
11. 分表字段不可更新。GORM `Update`、`Updates`、`Save` 和 Raw `UPDATE` 修改该字段会返回错误；需要调整分表时间时，请由业务显式执行“插入新分表并删除旧分表”。

## 性能说明

1. `MaxScanTables` 控制最多扫描多少张分表，不代表最多执行多少次元数据查询。
2. `plugin.AutoMigrate` 会对每张历史表执行 GORM 迁移检查，字段和索引越多，`information_schema` 查询越多。
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
