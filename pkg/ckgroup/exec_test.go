// +build integration

package ckgroup

import (
	"github.com/tal-tech/cds/pkg/ckgroup/dbtesttool/dbtool"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"testing"
	"time"
)

func Test_dbGroup_ExecAuto(t *testing.T) {
	group := MustCKGroup(c)
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, c.ShardGroups[0].ShardNode))

	err := ckTool.Clean()
	if err != nil {
		t.Fatal(err)
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Fatal(err)
	}
	dataSet := dbtool.GenerateDataSet(10000)
	var args [][]interface{}
	for _, item := range dataSet {
		args = append(args, []interface{}{
			item.PK,
			item.IntValue,
			item.FloatValue,
			item.DoubleValue,
			item.CharValue,
			item.VarCharValue,
			item.TimeValue,
		})
	}
	err = group.ExecAuto(insertCK, 0, args)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(time.Second * 2)
	statMap, err := ckTool.Select(dataSet)
	if err != nil {
		t.Fatal(err)
	}
	dbtool.DumpSelectInfo(statMap)
}

func Test_dbGroup_ExecAll(t *testing.T) {
	group := MustCKGroup(c)
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, c.ShardGroups[0].ShardNode))

	err := ckTool.Clean()
	if err != nil {
		t.Fatal(err)
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Fatal(err)
	}

	err = group.ExecAll(`alter table test.test_data add column temp String `, nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	for _, ckconn := range group.GetAllNodes() {
		cnt := struct {
			Cnt int `db:"cnt"`
		}{}
		err := ckconn.QueryRow(&cnt, `select count() cnt from system.columns where database = 'test'  and table = 'test_data' and name = 'temp'`)
		if err != nil {
			t.Fatal(err)
		}
		if cnt.Cnt != 1 {
			t.Fatal(`dbGroup.ExecAll is error ! `)
		}
	}
	err = group.ExecAll(`drop database if exists dbgroup_exec_test `, nil)
	if err != nil {
		t.Fatal(err)
	}
}
