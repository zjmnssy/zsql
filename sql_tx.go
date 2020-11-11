package zsql

import (
	"context"
	"database/sql"
	"fmt"
)

// TxBegin begin a sql tx.
func TxBegin(ctx context.Context, db *sql.DB, desc string) (*sql.Tx, error) {
	if db == nil {
		return nil, ErrorOfEmptyDB
	}

	if ctx == context.Background() || ctx == context.TODO() {
		return nil, ErrorOfEmptyCtx
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	var event EventItem
	event.Type = EventTypeOfTx
	event.Operate = EventOperateOfnNew
	event.Tx = tx
	event.Desc = desc
	GetManager().AddEvent(event)

	go func(ctx context.Context, tx *sql.Tx) {
		select {
		case <-ctx.Done():
			{
				err := TxRollback(tx) // 回滚已经commit的事务不会有影响
				if err != nil {
					fmt.Printf("rollback tx error = %s\n", err)
				}
			}
		}
	}(ctx, tx)

	return db.BeginTx(ctx, nil)
}

// TxRollback rollback tx.
func TxRollback(tx *sql.Tx) error {
	if tx == nil {
		return ErrorOfEmptyTX
	}

	var event EventItem
	event.Type = EventTypeOfTx
	event.Operate = EventOperateOfClose
	event.Tx = tx
	GetManager().AddEvent(event)

	return tx.Rollback()
}

// TxCommit commit tx.
func TxCommit(tx *sql.Tx) error {
	if tx == nil {
		return ErrorOfEmptyTX
	}

	var event EventItem
	event.Type = EventTypeOfTx
	event.Operate = EventOperateOfClose
	event.Tx = tx
	GetManager().AddEvent(event)

	return tx.Commit()
}
