// +build integration

package dbtool

import (
	"fmt"
	"testing"
)

func TestDBTestTool_UpdateMySQL(t *testing.T) {
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
	dataSet, err := mysqlTool.Update()
	if err != nil {
		fmt.Println("update data error")
		t.Error(err)
		return
	}
	statMap, err := mysqlTool.Select(dataSet)
	if err != nil {
		fmt.Println("select data error")
		t.Error(err)
		return
	}
	DumpSelectInfo(statMap)
}

func TestDBTestTool_UpdateCK(t *testing.T) {
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
	dataSet, err := ckTool.Update()
	if err != nil {
		fmt.Println("update data error")
		t.Error(err)
		return
	}
	statMap, err := ckTool.Select(dataSet)
	if err != nil {
		fmt.Println("select data error")
		t.Error(err)
		return
	}
	DumpSelectInfo(statMap)
}

func TestDBTestTool_UpdateBenchmark(t *testing.T) {
	suit := NewDBTestToolOnMySQL(mysqlDataSource)
	if err := suit.Clean(); err != nil {
		fmt.Println("clean db error")
		t.Error(err)
		return
	}
	if err := suit.SetUp(); err != nil {
		fmt.Println("set up error")
		t.Error(err)
		return
	}
	if _, err := suit.UpdateBenchmark(); err != nil {
		fmt.Println("update data error")
		t.Error(err)
		return
	}
}
