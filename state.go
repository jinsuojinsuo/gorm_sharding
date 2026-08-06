package gorm_sharding

import "gorm.io/gorm"

// copyQueryState 复制跨表查询需要保留的 GORM Statement 状态。
func copyQueryState(dst, src *gorm.DB) {
	// 跨分表执行时新建 Session，需要把用户链式调用积累的查询状态复制过去。
	dst.Statement.Model = src.Statement.Model
	dst.Statement.Clauses = src.Statement.Clauses
	dst.Statement.Selects = src.Statement.Selects
	dst.Statement.Omits = src.Statement.Omits
	// Distinct 是 Statement 独立状态，组合查询的 DryRun 构建也必须保留全局去重语义。
	dst.Statement.Distinct = src.Statement.Distinct
	dst.Statement.Joins = src.Statement.Joins
	dst.Statement.Preloads = src.Statement.Preloads
	dst.Statement.Settings = src.Statement.Settings
}

// copyWriteState 复制跨表 Update/Delete 需要保留的 GORM Statement 状态。
func copyWriteState(dst, src *gorm.DB) {
	copyQueryState(dst, src)
	dst.Statement.Dest = src.Statement.Dest
}

// copyCreateState 复制跨分表内部插入需要保留的用户配置。
func copyCreateState(dst, src *gorm.DB) {
	// OnConflict、Select 和 Omit 都记录在 Statement 中；拆分插入时必须继续生效。
	dst.Statement.Clauses = src.Statement.Clauses
	dst.Statement.Selects = src.Statement.Selects
	dst.Statement.Omits = src.Statement.Omits
	dst.Statement.Settings = src.Statement.Settings
}
