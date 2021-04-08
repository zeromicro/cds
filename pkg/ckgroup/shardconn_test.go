// +build integration

package ckgroup

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tal-tech/go-zero/core/stores/sqlx"

	"github.com/tal-tech/cds/pkg/ckgroup/config"
	"github.com/tal-tech/cds/pkg/ckgroup/dbtesttool/dbtool"
)

var shardGroupConfig = config.ShardGroupConfig{ShardNode: "tcp://localhost:9000", ReplicaNodes: []string{"tcp://localhost:9001"}}

func Test_shardConn_GetAllConn(t *testing.T) {
	shardConn, err := NewShardConn(1, shardGroupConfig)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(shardConn.GetAllConn()))
}

func Test_shardConn_GetReplicaConn(t *testing.T) {
	shardConn, err := NewShardConn(1, shardGroupConfig)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(shardConn.GetReplicaConn()))
}

func Test_shardConn_GetShardConn(t *testing.T) {
	shardConn, err := NewShardConn(1, shardGroupConfig)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, `localhost`, shardConn.GetShardConn().GetHost())
}

func Test_shardConn_InsertAuto(t *testing.T) {
	shardConn, err := NewShardConn(1, shardGroupConfig)
	if err != nil {
		t.Fatal(err)
	}
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, shardGroupConfig.ShardNode))

	err = ckTool.Clean()
	if err != nil {
		t.Fatal(err)
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Fatal(err)
	}
	dataSet := dbtool.GenerateDataSet(10000)

	err = shardConn.InsertAuto(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 3)
	statMap, err := ckTool.Select(dataSet)
	if err != nil {
		t.Fatal(err)
	}
	if !dbtool.DumpSelectInfo(statMap) {
		t.Error("data not same !")
	}
}

func Test_shardConn_ExecReplica(t *testing.T) {
	shardConn, err := NewShardConn(1, shardGroupConfig)
	if err != nil {
		t.Fatal(err)
	}
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, shardGroupConfig.ShardNode))

	err = ckTool.Clean()
	if err != nil {
		t.Fatal(err)
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Fatal(err)
	}
	dataSet := dbtool.GenerateDataSet(10000)

	err = shardConn.InsertAuto(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 3)
	errs := shardConn.ExecReplica(false, `alter table test.test_data delete where 1=?`, 1)
	if len(errs) != 0 {
		t.Fatal(fmt.Sprintf(`%+v`, errs))
	}
	cnt := struct {
		Cnt int `db:"cnt"`
	}{}
	time.Sleep(time.Second * 2)
	err = shardConn.GetShardConn().QueryRow(&cnt, `select count() cnt from test.test_data`)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cnt.Cnt)
	if cnt.Cnt != 0 {
		t.Fatal(`shardConn.ExecReplica is error ! `)
	}
}

func Test_shardConn_Exec(t *testing.T) {
	shardConn, err := NewShardConn(1, shardGroupConfig)
	if err != nil {
		t.Fatal(err)
	}
	errs := shardConn.Exec(false, `create database if not exists exec_test`)
	if len(errs) != 0 {
		t.Fatal(fmt.Sprintf(`%+v`, errs))
	}

	for _, ckconn := range shardConn.GetAllConn() {
		cnt := struct {
			Cnt int `db:"cnt"`
		}{}
		err := ckconn.QueryRow(&cnt, `select count() cnt from system.databases where name = 'exec_test'`)
		if err != nil {
			t.Fatal(err)
		}
		if cnt.Cnt != 1 {
			t.Fatal(`shardConn.Exec is error ! `)
		}
	}
}

func Test_shardConn_ExecAuto(t *testing.T) {
	shardConn, err := NewShardConn(1, shardGroupConfig)
	if err != nil {
		t.Fatal(err)
	}
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, shardGroupConfig.ShardNode))

	err = ckTool.Clean()
	if err != nil {
		t.Fatal(err)
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Fatal(err)
	}
	dataSet := dbtool.GenerateDataSet(10000)

	err = shardConn.InsertAuto(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)
	err = shardConn.ExecAuto(`alter table test.test_data delete where 1=?`, 1)
	if err != nil {
		t.Fatal(err)
	}
	cnt := struct {
		Cnt int `db:"cnt"`
	}{}
	time.Sleep(time.Second * 2)
	err = shardConn.GetShardConn().QueryRow(&cnt, `select count() cnt from test.test_data_all`)
	if err != nil {
		t.Fatal(err)
	}
	if cnt.Cnt != 0 {
		fmt.Println(cnt.Cnt)
		t.Fatal(`ckConn.Exec is error ! `)
	}
}

func Test_shardConn_New(t *testing.T) {
	if _, err := NewShardConn(1, shardGroupConfig); err != nil {
		t.Fatal(err)
		return
	}

	_, err := NewShardConn(1, config.ShardGroupConfig{ShardNode: "thost:9000", ReplicaNodes: []string{"tcp://localhost:9001"}})
	if err != hostParseErr {
		t.Fatal("err must be hostParseErr")
		return
	}
	if _, err := NewShardConn(1, config.ShardGroupConfig{ShardNode: "tcp://localhost:100000", ReplicaNodes: []string{"tcp://localhost:9001"}}); err != nil {
		t.Fatal(err)
		return
	}
}
