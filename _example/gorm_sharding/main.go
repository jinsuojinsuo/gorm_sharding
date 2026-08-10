package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jinsuojinsuo/gorm_sharding"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// User 演示按 created_at 字段进行月分表的业务模型。
type User struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name;type:varchar(64);not null;default:'';index"`
	Score     int       `gorm:"column:score;not null;default:0;index"`
	Score2    int       `gorm:"column:score2;not null;default:0;index"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime;not null;index"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime;not null"`
}

// TableName 返回 User 的逻辑表名；分表插件会用它作为真实分表前缀。
func (User) TableName() string {
	return "user"
}

// Order 演示同一个进程里第二张需要分表的业务表。
type Order struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	OrderNo   string    `gorm:"column:order_no;type:varchar(64);not null;default:'';uniqueIndex"`
	Amount    int64     `gorm:"column:amount;not null;default:0"`
	CreatedAt time.Time `gorm:"column:created_at;type:datetime;not null;index"`
	UpdatedAt time.Time `gorm:"column:updated_at;type:datetime;not null"`
}

// TableName 返回 Order 的逻辑表名；真实分表会是 order_202608 这类表名。
func (Order) TableName() string {
	return "order"
}

var MysqlUser = os.Getenv("MYSQL_USER")
var password = os.Getenv("MYSQL_PASSWORD")

// main 演示插件注册、自动迁移、增删改查和 Raw 查询的基本用法。
func main() {
	if MysqlUser == "" {
		fmt.Println("请出入mysql用户名")
		if _, err := fmt.Scanf("%s", &MysqlUser); err != nil {
			panic(err)
		}
	}

	if password == "" {
		fmt.Println("请出入mysql密码")
		if _, err := fmt.Scanf("%s", &password); err != nil {
			panic(err)
		}
	}

	db := connectMysql()

	plugin := gorm_sharding.New()
	// 同一个 DB 只创建一个插件实例；多张表需要分表时，对同一个插件 Register 多次。
	// 注意：所有 Register 都要在 db.Use(plugin) 之前完成。
	if err := plugin.Register(gorm_sharding.ShardingConfig{
		TablePrefix:     User{}.TableName(),         //逻辑表名，也是分表前缀
		ShardingKey:     "created_at",               //分表字段
		Strategy:        gorm_sharding.HourStrategy, //按小时分表
		Location:        time.Local,                 //固定分表时区
		MaxScanTables:   3,                          //最多扫描最近3个小时分表
		AutoCreateTable: true,                       //自动创建表
		AutoMigrate:     true,                       //自动迁移表
	}); err != nil {
		panic(err)
	}
	if err := plugin.Register(gorm_sharding.ShardingConfig{
		TablePrefix:     Order{}.TableName(),         //逻辑表名，也是分表前缀
		ShardingKey:     "created_at",                //分表字段
		Strategy:        gorm_sharding.MonthStrategy, //订单表按月分表
		Location:        time.Local,                  //固定分表时区
		MaxScanTables:   2,                           //无分表条件时最多扫描最近2张订单分表
		AutoCreateTable: true,                        //插入订单时自动创建目标分表
		AutoMigrate:     true,                        //调用 plugin.AutoMigrate 时迁移订单历史分表
	}); err != nil {
		panic(err)
	}

	//必须要先 plugin.Register再db.Use
	if err := db.Use(plugin); err != nil {
		panic(err)
	}

	if err := gorm_sharding.AutoMigrate(db, &User{}, &Order{}); err != nil {
		panic(err)
	}

	user(db)
	order(db)
}

func user(db *gorm.DB) {
	now := time.Now()
	//添加
	if err := db.Create(&User{
		Name:      "alice",
		Score:     100,
		CreatedAt: now,
		UpdatedAt: now,
	}).Error; err != nil {
		panic(err)
	}
	if err := db.Create(&User{
		Name:      "alice",
		Score:     100,
		CreatedAt: now.Add(-3600 * 2 * time.Second),
		UpdatedAt: now.Add(-3600 * 2 * time.Second),
	}).Error; err != nil {
		panic(err)
	}
	users := make([]User, 0)
	start := now.Add(-3600 * time.Second)
	end := now

	//查询
	if err := db.Where("created_at BETWEEN ? AND ?", start, end).Find(&users).Error; err != nil {
		panic(err)
	}
	fmt.Println("range query rows:", len(users))

	//修改
	if err := db.Model(&User{}).
		Where("created_at > ? AND created_at< ? AND name = ?", now.Add(-3600*time.Second), now, "alice").
		Updates(map[string]interface{}{
			"score":      120,
			"updated_at": time.Now(),
		}).Error; err != nil {
		panic(err)
	}

	//删除
	if err := db.Where("created_at = ? AND name = ?", now, "alice").Delete(&User{}).Error; err != nil {
		panic(err)
	}

	rawUsers := make([]User, 0)
	if err := db.Raw("SELECT * FROM user WHERE created_at = ?", now).Scan(&rawUsers).Error; err != nil {
		panic(err)
	}
	fmt.Println("raw query rows:", len(rawUsers))
}

func order(db *gorm.DB) {
	now := time.Now()
	if err := db.Create(&Order{
		OrderNo:   fmt.Sprintf("order_%d", now.UnixNano()),
		Amount:    9900,
		CreatedAt: now,
		UpdatedAt: now,
	}).Error; err != nil {
		panic(err)
	}

	orders := make([]Order, 0)
	if err := db.Where("created_at = ?", now).Find(&orders).Error; err != nil {
		panic(err)
	}
	fmt.Println("order query rows:", len(orders))
}

// connectMysql 创建示例用的 MySQL GORM 连接。
func connectMysql() *gorm.DB {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags|log.Lshortfile),
		logger.Config{
			SlowThreshold:             time.Millisecond * 500,
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", MysqlUser, password, "127.0.0.1", 3306, "test")
	db, err := gorm.Open(mysql.New(mysql.Config{
		DriverName: "mysql",
		DSN:        dsn,
	}), &gorm.Config{
		Logger: newLogger,
	})
	if err != nil {
		panic(err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		panic(err)
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetConnMaxLifetime(time.Hour)
	return db
}
