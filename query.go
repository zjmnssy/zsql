package zsql

import (
	"context"
	"database/sql"
	"fmt"
)

// QueryDb query a sql cmd by db.
func QueryDb(ctx context.Context, db *sql.DB, sqlStr string, args ...interface{}) (*sql.Rows, error) {
	if db == nil {
		return nil, ErrorOfEmptyDB
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return nil, ErrorOfEmptyCtx
	}

	rows, err := db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return rows, err
	}

	var event EventItem
	event.Type = EventTypeOfRows
	event.Operate = EventOperateOfnNew
	event.Desc = fmt.Sprintf(sqlStr, args...)
	event.Rows = rows
	GetManager().AddEvent(event)

	go func(ctx context.Context, rows *sql.Rows) {
		select {
		case <-ctx.Done():
			{
				err := CloseRows(rows)
				if err != nil {
					fmt.Printf("close rows error = %s\n", err)
				}
			}
		}
	}(ctx, rows)

	return rows, nil
}

// QueryTx query a sql cmd by tx.
func QueryTx(ctx context.Context, tx *sql.Tx, sqlStr string, args ...interface{}) (*sql.Rows, error) {
	if tx == nil {
		return nil, ErrorOfEmptyTX
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return nil, ErrorOfEmptyCtx
	}

	rows, err := tx.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return rows, err
	}

	var event EventItem
	event.Type = EventTypeOfRows
	event.Operate = EventOperateOfnNew
	event.Desc = fmt.Sprintf(sqlStr, args...)
	event.Rows = rows
	GetManager().AddEvent(event)

	go func(ctx context.Context, rows *sql.Rows) {
		select {
		case <-ctx.Done():
			{
				err := CloseRows(rows)
				if err != nil {
					fmt.Printf("close rows error = %s\n", err)
				}
			}
		}
	}(ctx, rows)

	return rows, nil
}

// Scan 从查询结果集中获取数据
func Scan(rows *sql.Rows, args ...interface{}) error {
	if rows == nil {
		return ErrorOfEmptyRows
	}

	err := rows.Scan(args...)
	if err != nil {
		errRows := CloseRows(rows)
		if errRows != nil {
			fmt.Printf("close rows error = %s\n", err)
		}
	}

	return err
}

// CloseRows close query rows.
func CloseRows(rows *sql.Rows) error {
	if rows == nil {
		return ErrorOfEmptyRows
	}

	var event EventItem
	event.Type = EventTypeOfRows
	event.Operate = EventOperateOfClose
	event.Rows = rows
	GetManager().AddEvent(event)

	return rows.Close()
}
