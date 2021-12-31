//go:build integration
// +build integration

package dbtool

import (
	"fmt"
	"testing"
)

func TestDBTestTool_Insert(t *testing.T) {
	mysqlTool := NewDBTestToolOnMySQL(mysqlDataSource)
	if err := mysqlTool.Clean(); err != nil {
		fmt.Println("mysql clean db error")
		t.Error(err)
		return
	}
	if err := mysqlTool.SetUp(); err != nil {
		fmt.Println("mysql set up error")
		t.Error(err)
		return
	}
	if _, err := mysqlTool.Insert(); err != nil {
		fmt.Println("mysql insert data error")
		t.Error(err)
		return
	}
	//ckTool := NewDBTestToolOnCK(ckDataSource)
	//if err := ckTool.Clean(); err != nil {
	//	fmt.Println("ck clean db error")
	//	t.Error(err)
	//	return
	//}
	//if err := ckTool.SetUp(); err != nil {
	//	fmt.Println("ck set up error")
	//	t.Error(err)
	//	return
	//}
	//if _, err := ckTool.Insert(); err != nil {
	//	fmt.Println("ck insert data error")
	//	t.Error(err)
	//	return
	//}
}

func TestDBTestTool_InsertBenchmarkMySQL(t *testing.T) {
	mysqlTool := NewDBTestToolOnMySQL(mysqlDataSource)
	if err := mysqlTool.Clean(); err != nil {
		fmt.Println("clean db error")
		t.Error(err)
		return
	}
	if err := mysqlTool.SetUp(); err != nil {
		fmt.Println("set up error")
		t.Error(err)
		return
	}
	if err := mysqlTool.InsertBenchmarkMySQL(); err != nil {
		fmt.Println("insert data error")
		t.Error(err)
		return
	}
}

func TestDBTestTool_InsertBenchmarkCK(t *testing.T) {
	ckTool := NewDBTestToolOnCK(ckDataSource)
	if err := ckTool.Clean(); err != nil {
		fmt.Println("clean db error")
		t.Error(err)
		return
	}
	if err := ckTool.SetUp(); err != nil {
		fmt.Println("set up error")
		t.Error(err)
		return
	}
	if err := ckTool.InsertBenchmarkCK(); err != nil {
		fmt.Println("insert data error")
		t.Error(err)
		return
	}
}
