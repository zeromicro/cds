//go:build integration
// +build integration

package dbtool

import "testing"

var (
	mysqlDataSource = "root:example@tcp(localhost:3306)/?parseTime=1&loc=Local&charset=utf8mb4&collation=utf8mb4_unicode_ci&multiStatements=true"
	ckDataSource    = "tcp://localhost:9000"
)

func TestDBTestTool_SetUp(t *testing.T) {
	mysqlTool := NewDBTestToolOnMySQL(mysqlDataSource)
	if err := mysqlTool.SetUp(); err != nil {
		t.Error(err)
		return
	}
	ckTool := NewDBTestToolOnCK(ckDataSource)
	if err := ckTool.SetUp(); err != nil {
		t.Error(err)
		return
	}
}

func TestDBTestTool_Clean(t *testing.T) {
	mysqlTool := NewDBTestToolOnMySQL(mysqlDataSource)
	if err := mysqlTool.Clean(); err != nil {
		t.Error(err)
		return
	}
	ckTool := NewDBTestToolOnCK(ckDataSource)
	if err := ckTool.Clean(); err != nil {
		t.Error(err)
		return
	}
}
