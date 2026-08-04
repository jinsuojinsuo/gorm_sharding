package gorm_sharding

import "gorm.io/gorm"

// copyQueryState 复制跨表查询需要保留的 GORM Statement 状态。
func copyQueryState(dst, src *gorm.DB) {
	// 跨分表执行时新建 Session，需要把用户链式调用积累的查询状态复制过去。
	dst.Statement.Model = src.Statement.Model
	dst.Statement.Clauses = src.Statement.Clauses
	dst.Statement.Selects = src.Statement.Selects
	dst.Statement.Omits = src.Statement.Omits
	dst.Statement.Joins = src.Statement.Joins
	dst.Statement.Preloads = src.Statement.Preloads
	dst.Statement.Settings = src.Statement.Settings
}

// copyWriteState 复制跨表 Update/Delete 需要保留的 GORM Statement 状态。
func copyWriteState(dst, src *gorm.DB) {
	copyQueryState(dst, src)
	dst.Statement.Dest = src.Statement.Dest
}
