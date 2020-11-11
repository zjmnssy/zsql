package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zjmnssy/zsql"

	_ "github.com/go-sql-driver/mysql"
)

type exitFunc func()

func securityExitProcess(exitFunc exitFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for s := range c {
		switch s {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			fmt.Printf("\n[ INFO ] (system) - security exit by %s signal.\n", s)
			exitFunc()
		default:
			fmt.Printf("\n[ INFO ] (system) - unknown exit by %s signal.\n", s)
			exitFunc()
		}
	}
}

func quit() {
	os.Exit(0)
}

func do() {
	var c zsql.Config
	c.SQLName = "mysql"
	c.Account = "root"
	c.Password = "meilimysqltest"
	c.Address = "10.10.3.58:3306"
	c.Database = "config_server"
	c.MaxIdleNumber = 32
	c.MaxOpenNumber = 64
	c.MaxLifeTime = 300
	c.ScanPeriod = 1
	c.EventTimeout = 60

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(3000)*time.Millisecond)
	defer cancel()

	db, err := zsql.GetDB(c)
	if err != nil {
		fmt.Printf("connect to db error = %s\n", err)
		return
	}

	defer zsql.CloseDb(db)

	rows, err := zsql.QueryDb(ctx, db, "select id,accunt from member order by id asc limit 0,10;")
	if err != nil {
		fmt.Printf("db query error = %s\n", err)
		return
	}

	for rows.Next() {
		var userID int
		var username string
		err = zsql.Scan(rows, &userID, &username)
		if err != nil {
			fmt.Printf("rows scan error = %s\n", err)
			break
		}

		fmt.Printf("userID = %d  username = %s\n", userID, username)

		break
	}

	time.Sleep(time.Duration(3) * time.Second)

	zsql.ShowDbStatus(db)
}

func main() {
	do()

	securityExitProcess(quit)
}
