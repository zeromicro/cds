package tube

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
)

// Kfk2Db  simple pull kafka data to clickhouse
func Kfk2Db(ctx context.Context, sc KfkStreamConsumer, db *sql.DB, num int, h HandleFunc, query string) error {
	err := errors.New("outoff loop")
	stream := sc.Subscribe(ctx, h)
	defer sc.Close()
	cnt := 0
transporting:
	for {
		tx, stmt, err := prepareDb(db, query)
		if err != nil {
			break transporting
		}

	perStream:
		for data := range stream {
			if cnt++; cnt >= num {
				cnt = 0
				break perStream
			}
			columns := data.([]interface{})
			if _, err := stmt.Exec(columns...); err != nil {
				logx.Error(err)
				backUp()
			}
		}
		select {
		case <-ctx.Done():
			return errors.New("cancel() ")
		default:
		}
		if err := tx.Commit(); err != nil {
			logx.Error(err)
			backUp()
		}
		_ = sc.Commit()
		time.Sleep(time.Second * 3)
	}
	return err
}

func prepareDb(db *sql.DB, query string) (*sql.Tx, *sql.Stmt, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, nil, err
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, nil, err
	}
	return tx, stmt, nil
}

// todo
func backUp() {

}
