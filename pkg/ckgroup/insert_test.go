// +build integration

package ckgroup

import (
	"github.com/tal-tech/cds/pkg/ckgroup/dbtesttool/dbtool"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"testing"
	"time"
)

func Test_dbGroup_InsertAuto(t *testing.T) {
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
	err = group.InsertAuto(insertCK2, "pk", dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)
	statMap, err := ckTool.Select(dataSet)
	if err != nil {
		t.Fatal(err)
	}
	dbtool.DumpSelectInfo(statMap)
}
