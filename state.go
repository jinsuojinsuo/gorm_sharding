package gorm_sharding

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// copyQueryState 复制跨表查询需要保留的 GORM Statement 状态。
func copyQueryState(dst, src *gorm.DB) {
	// 跨分表执行时新建 Session，需要把用户链式调用积累的查询状态复制过去。
	dst.Statement.Model = src.Statement.Model
	// Unscoped 是 Statement 级状态；跨分表新建 Session 后必须保留，
	// 否则软删除模型会错误追加 deleted_at IS NULL，Unscoped Delete 也会退化为软删除。
	dst.Statement.Unscoped = src.Statement.Unscoped
	dst.Statement.Clauses = cloneStatementClauses(src.Statement.Clauses)
	dst.Statement.Selects = append([]string(nil), src.Statement.Selects...)
	dst.Statement.Omits = append([]string(nil), src.Statement.Omits...)
	// Distinct 是 Statement 独立状态，组合查询的 DryRun 构建也必须保留全局去重语义。
	dst.Statement.Distinct = src.Statement.Distinct
	dst.Statement.Joins = append(dst.Statement.Joins[:0], src.Statement.Joins...)
	dst.Statement.Preloads = cloneStatementPreloads(src.Statement.Preloads)
	copyStatementSettings(dst.Statement, src.Statement)
}

// copyWriteState 复制跨表 Update/Delete 需要保留的 GORM Statement 状态。
func copyWriteState(dst, src *gorm.DB) {
	copyQueryState(dst, src)
	dst.Statement.Dest = src.Statement.Dest
}

// copyCreateState 复制跨分表内部插入需要保留的用户配置。
func copyCreateState(dst, src *gorm.DB) {
	// OnConflict、Select 和 Omit 都记录在 Statement 中；拆分插入时必须继续生效。
	dst.Statement.Clauses = cloneStatementClauses(src.Statement.Clauses)
	dst.Statement.Selects = append([]string(nil), src.Statement.Selects...)
	dst.Statement.Omits = append([]string(nil), src.Statement.Omits...)
	copyStatementSettings(dst.Statement, src.Statement)
}

// cloneStatementClauses 复制 Clause map，防止内部 GORM 回调修改调用方的条件状态。
func cloneStatementClauses(source map[string]clause.Clause) map[string]clause.Clause {
	cloned := make(map[string]clause.Clause, len(source))
	for name, value := range source {
		cloned[name] = value
	}
	return cloned
}

// cloneStatementPreloads 复制预加载配置容器，避免内部 Session 和调用方共享 map。
func cloneStatementPreloads(source map[string][]interface{}) map[string][]interface{} {
	cloned := make(map[string][]interface{}, len(source))
	for name, values := range source {
		cloned[name] = append([]interface{}(nil), values...)
	}
	return cloned
}

// copyStatementSettings 使用 Range 和 Store 复制 sync.Map，sync.Map 在使用后不能直接赋值复制。
func copyStatementSettings(dst, src *gorm.Statement) {
	dst.Settings.Range(func(key, value interface{}) bool {
		dst.Settings.Delete(key)
		return true
	})
	src.Settings.Range(func(key, value interface{}) bool {
		dst.Settings.Store(key, value)
		return true
	})
}
