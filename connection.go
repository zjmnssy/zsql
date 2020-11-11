package zsql

import (
	"database/sql"
	"fmt"
	"net/url"
	"time"
)

const (
	defaultMaxIdleNum  = 100
	defaultMaxOpenNum  = 200
	defaultMaxLifeTime = 60 * 30
)

// Config of db.
type Config struct {
	SQLName  string `json:"sqlName"`
	Account  string `json:"account"`
	Password string `json:"password"`
	Address  string `json:"address"`
	Database string `json:"database"`

	// the maximum number of connections in the idle　connection pool.
	MaxIdleNumber int `json:"maxIdleNum"`

	// the maximum number of open connections to the database.
	MaxOpenNumber int `json:"maxOpenNum"`

	// the maximum amount of time a connection may be reused, per - second.
	MaxLifeTime time.Duration `json:"maxLifeTime"`

	// per - second.
	ScanPeriod time.Duration `json:"scanPeriod"`

	// per - second.
	EventTimeout time.Duration `json:"eventTimeout"`
}

func initDb(db *sql.DB, c Config) error {
	if db == nil {
		return ErrorOfEmptyDB
	}

	if c.MaxIdleNumber <= 0 {
		c.MaxIdleNumber = defaultMaxIdleNum
	}

	if c.MaxOpenNumber <= 0 {
		c.MaxOpenNumber = defaultMaxOpenNum
	}

	if c.MaxLifeTime <= 0 {
		c.MaxLifeTime = defaultMaxLifeTime
	}

	if c.ScanPeriod <= 0 {
		c.ScanPeriod = defaultEventManagerPeriod
	}

	if c.EventTimeout <= 0 {
		c.EventTimeout = defaultEventManagerTimeout
	}

	db.SetMaxIdleConns(c.MaxIdleNumber)
	db.SetMaxOpenConns(c.MaxOpenNumber)
	db.SetConnMaxLifetime(c.MaxLifeTime * time.Second)

	GetManager().SetConfig(c.ScanPeriod, c.EventTimeout)

	return nil
}

// GetDB to db server, note:PingContext need use version not low than
// github.com/go-sql-driver/mysql v1.4.1-0.20191212001955-b66d043e6c89,
// otherwise timeout will not effect.
func GetDB(c Config) (*sql.DB, error) {
	timezone := "'Asia/Shanghai'"
	startStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local&time_zone=%s",
		c.Account, c.Password, c.Address, c.Database, url.QueryEscape(timezone))
	db, err := sql.Open(c.SQLName, startStr)
	if err != nil {
		return nil, err
	}

	err = initDb(db, c)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// ShowDbStatus show database handle state.
func ShowDbStatus(db *sql.DB) error {
	if db == nil {
		return ErrorOfEmptyDB
	}

	state := db.Stats()
	fmt.Printf("\n******************************** Db info ********************************\n"+
		"* Maximum number of open connections to the database : %d\n"+
		"* \n"+
		"* ---------------- pool state ----------------\n"+
		"* The number of established connections both in use and idle : %d\n"+
		"* The number of connections currently in use : %d\n"+
		"* The number of idle connections : %d\n"+
		"* \n"+
		"* ---------------- statistics info -----------\n"+
		"* The total number of connections waited for : %d\n"+
		"* The total time blocked waiting for a new connection : %d\n"+
		"* The total number of connections closed due to SetMaxIdleConns : %d\n"+
		"* The total number of connections closed due to SetConnMaxLifetime : %d\n"+
		"*************************************************************************\n",
		state.MaxOpenConnections, state.OpenConnections, state.InUse, state.Idle,
		state.WaitCount, state.WaitDuration, state.MaxIdleClosed, state.MaxLifetimeClosed)

	return nil
}

// CloseDb close database handle.
func CloseDb(db *sql.DB) error {
	if db == nil {
		return ErrorOfEmptyDB
	}

	err := db.Close()
	if err != nil {
		fmt.Printf("close db error = %s\n", err)
	} else {
		fmt.Printf("notice db now to be close\n")
	}

	return err
}
