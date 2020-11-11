package zsql

import (
	"context"
	"database/sql"
	"fmt"
)

// LockForWrite 对表加写锁.
// 表共享读锁，独占写锁.
// 如果数据库表被锁，此时，此db句柄对此表的读写操作都可正常执行，其他db句柄对此表的读写操作将被阻塞直到锁释放.
func LockForWrite(ctx context.Context, db *sql.DB, tableName string) error {
	if db == nil {
		return ErrorOfEmptyDB
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return ErrorOfEmptyCtx
	}

	var sqlStr = fmt.Sprintf("LOCK TABLES %s WRITE;", tableName)
	_, err := db.ExecContext(ctx, sqlStr)

	return err
}

// LockForRead 对表加读锁.
// 表共享读锁，独占写锁.
// 如果数据库表被锁，此时，此db句柄和其他db句柄对此表的写操作都将被阻塞直到锁释放.
// 如果数据库表被锁，此时，此db句柄和其他db句柄对此表对此表的读操作不受影响.
func LockForRead(ctx context.Context, db *sql.DB, tableName string) error {
	if db == nil {
		return ErrorOfEmptyDB
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return ErrorOfEmptyCtx
	}

	var sqlStr = fmt.Sprintf("LOCK TABLES %s READ;", tableName)
	_, err := db.ExecContext(ctx, sqlStr)

	return err
}

// UnLockTable 释放表的读写锁.
// (将释放此db对象锁住的所有表)
func UnLockTable(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrorOfEmptyDB
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return ErrorOfEmptyCtx
	}

	var sqlStr = fmt.Sprintf("UNLOCK TABLES;")
	_, err := db.ExecContext(ctx, sqlStr)
	return err
}
