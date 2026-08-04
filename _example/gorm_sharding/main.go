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

// main 演示插件注册、自动迁移、增删改查和 Raw 查询的基本用法。
func main() {
	db := connectMysql()

	plugin := gorm_sharding.New()
	if err := plugin.Register(User{}, gorm_sharding.ShardingConfig{
		ShardingKey:     "created_at",               //分表字段
		Strategy:        gorm_sharding.HourStrategy, //按小时分表
		TablePrefix:     "user",                     //表名前缀
		MaxScanTables:   3,                          //最大扫描10张表
		AutoCreateTable: true,                       //自动创建表
		AutoMigrate:     true,                       //自动迁移表
	}); err != nil {
		panic(err)
	}

	if err := db.Use(plugin); err != nil {
		panic(err)
	}

	if err := plugin.AutoMigrate(db, &User{}); err != nil {
		panic(err)
	}

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
	start := now.AddDate(0, -1, 0)
	end := now.AddDate(0, 1, 0)

	//查询
	if err := db.Where("created_at BETWEEN ? AND ?", start, end).Find(&users).Error; err != nil {
		panic(err)
	}
	fmt.Println("range query rows:", len(users))

	//修改
	if err := db.Model(&User{}).
		Where("created_at = ? AND name = ?", now, "alice").
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

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", "root", "123456", "127.0.0.1", 3306, "test")
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
