package zsql

import (
	"context"
	"database/sql"
)

// ExecDb exec a sql cmd by db.
func ExecDb(ctx context.Context, db *sql.DB, sqlStr string, args ...interface{}) (sql.Result, error) {
	if db == nil {
		return nil, ErrorOfEmptyDB
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return nil, ErrorOfEmptyCtx
	}

	return db.ExecContext(ctx, sqlStr, args...)
}

// ExecTx exec a sql cmd by tx.
func ExecTx(ctx context.Context, tx *sql.Tx, sqlStr string, args ...interface{}) (sql.Result, error) {
	if tx == nil {
		return nil, ErrorOfEmptyTX
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return nil, ErrorOfEmptyCtx
	}

	return tx.ExecContext(ctx, sqlStr, args...)
}
