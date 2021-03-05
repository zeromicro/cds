// +build integration

package ckgroup

import (
	"database/sql"
	"fmt"
	"testing"
)

func Test_saveData(t *testing.T) {
	conn, err := sql.Open(`clickhouse`, `tcp://localhost:9000`)
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = conn.Exec(`drop table test_insert`)
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = conn.Exec(`create table test_insert
(
    a Int64,
    b Int64
) engine = Memory `)

	if err != nil {
		t.Fatal(err.Error())
	}
	failInsert(conn, t)
	successInsert(conn, t)
}

func successInsert(conn *sql.DB, t *testing.T) {
	args := []rowValue{}
	for i := 0; i < 10; i++ {
		args = append(args, []interface{}{i, i})
	}
	err := saveData(conn, `insert into test_insert (a,b) values (?,?)`, args)
	if err != nil {
		t.Fatal(err.Error())
	}

	row := conn.QueryRow(`select toInt64(count()) cnt from  test_insert`)
	var cnt int64
	err = row.Scan(&cnt)
	if err != nil {
		t.Fatal(err.Error())
	}
	if cnt != 10 {
		t.Fatal(fmt.Sprintf("actual count:%d,expect count:10\n", cnt))
	}
}

func failInsert(conn *sql.DB, t *testing.T) {
	args := []rowValue{}
	for i := 0; i < 10; i++ {
		args = append(args, []interface{}{i, i})
	}
	// 错误的参数数量
	args = append(args, []interface{}{1})
	_ = saveData(conn, `insert into test_insert (a,b) values (?,?)`, args)

	row := conn.QueryRow(`select toInt64(count()) cnt from  test_insert`)
	var cnt int64
	err := row.Scan(&cnt)
	if err != nil {
		t.Fatal(err.Error())
	}
	if cnt != 0 {
		t.Fatal(fmt.Sprintf("actual count:%d,expect count:0\n", cnt))
	}
}
