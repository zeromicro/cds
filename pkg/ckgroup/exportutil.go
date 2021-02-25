package ckgroup

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/tal-tech/go-zero/core/logx"
)

// Deprecated
func DataChanInsert(db *sql.DB, dataChan chan interface{}, insertSql string, length int) error {
	i := 0
	var values [][]interface{}
	for {
		message, ok := <-dataChan
		if !ok {
			return saveData(db, insertSql, values)
		} else {
			i++
			v := reflect.ValueOf(message)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			var value []interface{}
			for i := 0; i < v.NumField(); i++ {
				value = append(value, v.Field(i).Interface())
			}
			values = append(values, value)
			if i%length == 0 {
				if err := saveData(db, insertSql, values); err != nil {
					logx.Error(err.Error())
					return err
				}
				values = values[0:0]
			}
		}
	}
}

// Deprecated: Use CKConn.QueryStream instead.
func BatchScanRows(db *sql.DB, ch chan interface{}, dest interface{}, query string, args ...interface{}) error {
	dataType := reflect.TypeOf(dest)
	if dataType.Kind().String() != "struct" {
		return errors.New("struct element must be struct")
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	names, err := rows.Columns()
	if err != nil {
		return err
	}
	columnIdx := make([]int, len(names))
	for i := range columnIdx {
		columnIdx[i] = -1
	}

	for i := 0; i < dataType.NumField(); i++ {
		tagValue := dataType.Field(i).Tag.Get(DbTag)
		for nameIdx, name := range names {
			if tagValue == name {
				columnIdx[nameIdx] = i
			}
		}
	}
	go func() {
		defer close(ch)
		defer rows.Close()

		for rows.Next() {
			v := reflect.New(dataType)
			err := rows.Scan(span(v.Interface(), columnIdx)...)
			if err != nil {
				logx.Error(err)
				break
			}
			ch <- v.Interface()
		}
	}()

	return nil
}
