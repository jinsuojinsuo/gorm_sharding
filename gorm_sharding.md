# GORM 分表插件需求文档 v1.0

## 1. 项目目标

开发一个基于：

```go
gorm.io/gorm
```

最新版的分表插件，实现业务代码无感知分表。

业务层保持和普通 GORM 单表操作一致：

```go
db.Create(&User{})

db.Where("id=?", 1).Find(&user)

db.Model(&User{}).
   Where(...).
   Updates(...)

db.Delete(&User{}, id)
```

插件自动完成：

- 根据分表字段路由真实表
- 动态修改 SQL
- 自动创建分表
- 自动迁移历史分表
- 支持跨分表查询
- 保持 GORM 原有返回结果

---

# 2. 技术要求

## 2.1 GORM版本

使用：

```go
gorm.io/gorm
```

最新版本。


---

## 2.2 SQL解析

使用：

```go
vitess.io/vitess/go/vt/sqlparser
```

进行 SQL AST 解析。


禁止：

- 正则替换 SQL
- 字符串拼接 SQL


---

# 3. 支持数据库

第一版：

支持：

- MySQL


---

# 4. 分表规则

支持以下分表方式：


|规则|示例|
|-|-|
|年|user_2026|
|月|user_202608|
|周|user_2026_w32|
|日|user_20260804|
|小时|user_2026080413|


---

# 5. 分表字段


第一版限制：

只支持：

```go
time.Time
```


数据库字段：

支持：

```sql
DATETIME

TIMESTAMP
```


不支持：

- int时间戳
- string日期
- 表达式计算


---

# 6. 分表配置


## 6.1 配置结构


```go
type ShardingConfig struct {

    // 分表字段
    ShardingKey string


    // 分表策略
    Strategy ShardingStrategy


    // 最大扫描表数量
    MaxScanTables int


    // 是否自动创建表
    AutoCreateTable bool


    // 是否自动迁移
    AutoMigrate bool
}
```

真实分表前缀不再放在配置中，插件会从 GORM 结构体逻辑表名中获取。模型实现 `TableName()` 时使用该返回值；未实现时使用 GORM 默认命名规则。


---

## 6.2 注册方式


示例：

```go
plugin.Register(
    User{},
    ShardingConfig{
        ShardingKey:"created_at",

        Strategy:
            MonthStrategy,

        MaxScanTables:
            10,

        AutoCreateTable:
            true,

        AutoMigrate:
            true,
    },
)
```


---

# 7. 表结构管理


## 7.1 无模板表


不创建：

```sql
user
```


只存在：

```sql
user_202608
user_202609
```


---

## 7.2 表结构来源


不自己生成 CREATE SQL。


使用 GORM Migrator：

```go
db.Table(tableName).
   AutoMigrate(&User{})
```


优点：

支持：

- gorm tag
- index
- unique
- comment
- default
- charset
- engine


---

# 8. 自动创建分表


## 8.1 INSERT触发创建


流程：

```
Create

↓

获取struct

↓

读取分表字段

↓

计算目标表

↓

检查表是否存在

↓

不存在

↓

Table(target).
AutoMigrate()

↓

执行insert
```


---

## 8.2 不提前创建未来表


不主动创建：

例如：

当前：

```
user_202608
```

不会创建：

```
user_202609
```


可扩展：

未来增加定时创建机制。


---

# 9. 自动迁移


## 9.1 支持历史表字段同步


例如：

原结构：

```go
type User struct {

    ID uint64

    Name string

}
```


已有：

```
user_202601
user_202602
```


增加：

```go
Age int
```


执行：

```go
db.AutoMigrate(&User{})
```


插件自动：

```
扫描最近 MaxScanTables 张分表

↓

逐个执行:

Table(table).
AutoMigrate(User{})
```


结果：

最近 MaxScanTables 张历史表增加字段。


---

## 9.2 新表自动包含最新结构


未来：

```
user_202609
```

创建时：

直接使用最新 struct。


---

# 10. CRUD支持


支持：

|操作|支持|
|-|-|
|Create|√|
|Select|√|
|Update|√|
|Delete|√|
|Raw单表SQL|√|
|Join|×|


---

# 11. INSERT设计


## 单条插入


业务：

```go
db.Create(&User{
    CreatedAt:t,
})
```


内部：

生成：

```sql
INSERT INTO user_202608
```


---

## 批量插入


支持：

```go
db.Create([]User{})
```


数据：

```
20260801
20260801
20260802
```


自动拆分：

```
insert user_20260801

insert user_20260802
```


---

# 12. SELECT设计


## 12.1 包含分表字段


例如：

```go
Where(
"created_at between ? and ?",
start,
end,
)
```


计算：

```
user_20260801
user_20260802
```


只查询对应表。


---

## 12.2 不包含分表字段


例如：

```go
Where(
"name=?",
"abc",
)
```


不能扫描全部历史表。


使用：

```go
MaxScanTables
```


例如：

```go
MaxScanTables:10
```


只扫描：

```
最近10张表
```


---

# 13. 多表查询结果处理


默认方案：

## Go层合并


例如：

查询：

```
user_20260801
user_20260802
```


分别执行：

```sql
select *
from user_20260801


select *
from user_20260802
```


结果：

Go合并：

```go
[]User
```


保持GORM体验。


---

# 14. Order + Limit 查询


如果：

```go
db.
Order("score desc").
Limit(20).
Find(&users)
```


不能简单Go合并。


生成：

```sql
(
 SELECT *
 FROM user_20260801
 ORDER BY score DESC
 LIMIT 20
)

UNION ALL

(
 SELECT *
 FROM user_20260802
 ORDER BY score DESC
 LIMIT 20
)

ORDER BY score DESC

LIMIT 20
```


原因：

每个分表先取Top N，避免全表扫描。

支持 Offset：每张分表先取 `Offset + Limit` 行，外层再统一执行原 Offset 和 Limit。

跨分表 Group By 和聚合也通过相同的 UNION ALL 外层查询实现：内层合并各真实分表原始行，外层执行原始 SELECT、GROUP BY、HAVING、ORDER BY、LIMIT，因此 COUNT、SUM、MIN、MAX、AVG 按全部命中分表的数据计算。

限制：只支持单模型、无 Join 的 GORM 查询；Group、Order、Select、Having 中不要手写逻辑表限定名，例如 `user.score`，应使用 `score`。


---

# 15. UPDATE设计


## 有分表字段


例如：

```go
Where(
"created_at=?",
time
)
```


只更新目标表。


---

## 无分表字段


例如：

```go
Updates(...)
```


扫描：

```
最近MaxScanTables张表
```


执行：

多个update。


RowsAffected：

所有表累加。


---

## 表不存在


返回：

```go
RowsAffected=0

Error=nil
```


---

# 16. DELETE设计


和UPDATE一致。


支持：

- 分表字段定位
- 最近N表扫描
- RowsAffected累加


---

# 17. Raw支持


支持：

```go
db.Raw(
"select * from user where id=?",
1,
)
```


处理：

```
Raw SQL

↓

sqlparser解析

↓

替换table

↓

执行
```


---

不支持：

复杂Join：

例如：

```sql
select *
from user
join order
```


原因：

多分表Join组合复杂。

Raw/Exec 只支持目标明确的一张真实分表。条件命中多张分表时返回错误，不通过 `multiStatements=true` 拼接多条 SQL 执行。


---

# 18. SQL改写方案


流程：

```
GORM生成SQL

↓

Callback

↓

sqlparser.Parse()

↓

AST分析

↓

计算目标表

↓

替换TableName

↓

sqlparser.String()

↓

执行
```


---

支持解析：

- SELECT
- INSERT
- UPDATE
- DELETE


---

# 19. GORM Callback设计


注册：

Create:

```
before_create
```


Query:

```
query callback
```


Update:

```
update callback
```


Delete:

```
delete callback
```


---

# 20. Migrator设计


不修改GORM源码。


通过：

```
GORM Migrator
```

包装：

实现：

- AutoMigrate接管
- CreateTable路由
- Alter Table同步


---

# 21. 插件内部结构


目录：

```
gorm-sharding/


config.go


plugin.go


strategy/

    year.go
    month.go
    week.go
    day.go
    hour.go


callback/

    create.go
    query.go
    update.go
    delete.go


parser/

    select.go
    insert.go
    update.go
    delete.go


migrator/

    migrator.go


table/

    manager.go


utils/

    time.go
```


---

# 22. 第一版明确不支持


不支持：

- Join跨分表
- SQL表达式分表字段计算
- 多数据库


---

# 23. 第一版验收标准


## INSERT

- 自动计算分表
- 自动建表
- 自动插入


## SELECT

- 精确条件只查目标表
- 无条件最多扫描N张表
- 多表结果正常返回


## UPDATE

- 精确更新目标表
- 无条件扫描限制
- RowsAffected正确


## DELETE

同UPDATE。


## AutoMigrate

- 新字段同步所有历史分表
- 新表自动拥有最新结构


---

# 24. 后续扩展方向

预留：

- 分库
- Hash分表
- 多字段分片
- Join分表
- 聚合查询
- 热冷数据迁移
- 自动清理历史表


---

以上为最终 v1.0 需求文档。  
后续实现阶段建议顺序：

1. 先实现表路由 + Insert自动建表
2. 实现Select多表查询
3. 实现Update/Delete
4. 实现AutoMigrate包装
5. 最后补Raw SQL支持和复杂优化。
