package zsql

import "errors"

var ErrorOfEmptyDB = errors.New("db is nil")

var ErrorOfEmptyCtx = errors.New("ctx can't be type of Background() and TODO()")

var ErrorOfEmptyTX = errors.New("tx is nil")

var ErrorOfEmptyRows = errors.New("rows is nil")
