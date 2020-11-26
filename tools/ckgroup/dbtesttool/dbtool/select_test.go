// +build integration

package dbtool

import (
	"fmt"
	"testing"
	"time"
)

func TestDBTestTool_SelectMySQL(t *testing.T) {
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
	dataSet, err := mysqlTool.Insert()
	if err != nil {
		fmt.Println("insert data error")
		t.Error(err)
		return
	}
	result, err := mysqlTool.Select(dataSet)
	if err != nil {
		fmt.Println("select data error")
		t.Error(err)
		return
	}
	DumpSelectInfo(result)
}

func TestDBTestTool_SelectCK(t *testing.T) {
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
	dataSet, err := ckTool.Insert()
	if err != nil {
		fmt.Println("insert data error")
		t.Error(err)
		return
	}
	time.Sleep(time.Second * 3)
	result, err := ckTool.Select(dataSet)
	if err != nil {
		fmt.Println("select data error")
		t.Error(err)
		return
	}
	DumpSelectInfo(result)
}
