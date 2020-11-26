// +build integration

package dbtool

import (
	"fmt"
	"testing"
)

func TestDBTestTool_Delete(t *testing.T) {
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
	if _, err := suit.Delete(); err != nil {
		fmt.Println("update data error")
		t.Error(err)
		return
	}
}
