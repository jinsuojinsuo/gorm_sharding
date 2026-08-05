# gorm_sharding

`gorm_sharding` 是一个基于 GORM 的 MySQL 时间分表插件。业务代码继续使用普通 GORM 的 `Create`、`Find`、`Updates`、`Delete`、`Raw` 写法，插件负责根据时间字段路由到真实分表。

## 功能

1. 支持按年、月、周、日、小时分表。
2. 支持 `time.Time` 分表字段，字段名可使用 Go 字段名或数据库列名。
3. 插入时自动计算目标表，并在表不存在时使用 GORM `AutoMigrate` 自动创建。
4. 批量插入会按目标分表自动拆分。
5. 查询包含分表字段时精确路由；不包含分表字段时最多扫描最近 `MaxScanTables` 张表。
6. 跨表查询在 Go 层合并结果，保持 GORM slice 返回体验。
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
	// 分表字段，支持 Go 字段名或数据库列名，例如 CreatedAt 或 created_at。
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

## 自动迁移

历史分表字段同步使用插件提供的迁移方法：

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
game:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local
```

运行前请确认本机 MySQL 账号、密码和数据库可用。

## 测试

运行需求验收测试：

```bash
go test ./... -run TestRequirement -count=1 -v
```

测试默认连接：

```text
game:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local
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
5. 跨分表 Group By 不支持。
6. 跨分表 Order + Limit 不支持。
7. Raw SQL 只支持单表 SQL 的表名改写，不支持复杂 Join 组合。
8. 不接管 `db.AutoMigrate`，历史分表迁移请使用 `plugin.AutoMigrate`。

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
