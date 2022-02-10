//go:build integration
// +build integration

package ckgroup

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/cds/pkg/ckgroup/dbtesttool/dbtool"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

const (
	queryNodeDNS = `tcp://localhost:9000`
)

func TestMain(m *testing.M) {
	logx.Disable()
	m.Run()
}

func Test_ckConn_GetHost(t *testing.T) {
	c1, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, `localhost`, c1.GetHost())
	_, err = NewCKConn("")
	if err == nil {
		t.Fatal("err is nil ! ")
	}
	_ = c1.GetRawConn().Close()
}

func Test_ckConn_GetRawConn(t *testing.T) {
	conn, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	if conn.GetRawConn() == nil {
		t.Fatal("GetRawConn return is nil !")
	}
	_ = conn.GetRawConn().Close()
}

func Test_ckConn_GetUser(t *testing.T) {
	c1, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	logx.Disable()
	assert.Equal(t, ``, c1.GetUser())
	_ = c1.GetRawConn().Close()
	c2, err := NewCKConn("tcp://localhost:9000?username=default")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, `default`, c2.GetUser())
	_ = c2.GetRawConn().Close()
}

func Test_ckConn_Insert(t *testing.T) {
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, queryNodeDNS))

	err := ckTool.Clean()
	if err != nil {
		t.Error(err)
		return
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Error(err)
		return
	}
	dataSet := dbtool.GenerateDataSet(10000)

	ckConn, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	err = ckConn.Insert(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 3)
	statMap, err := ckTool.Select(dataSet)
	if err != nil {
		t.Error(err)
		return
	}
	if !dbtool.DumpSelectInfo(statMap) {
		t.Error("data not same !")
	}
}

func Test_ckConn_QueryRow(t *testing.T) {
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, queryNodeDNS))
	err := ckTool.Clean()
	if err != nil {
		t.Error(err)
		return
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Error(err)
		return
	}
	dataSet := dbtool.GenerateDataSet(100)

	ckConn, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	err = ckConn.Insert(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)
	for i := 0; i < 100; i++ {
		pk := dataSet[i].PK
		query := "select * from test.test_data_all where pk=?"
		var result dbtool.DataInstance
		err = ckConn.QueryRow(&result, query, pk)
		if err != nil {
			t.Error(err)
			return
		}
		if !dbtool.Compare(dataSet[i], &result, true) {
			t.Errorf("data is not consistent")
			return
		}
	}
}

func Test_ckConn_QueryRows(t *testing.T) {
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, queryNodeDNS))

	err := ckTool.Clean()
	if err != nil {
		t.Error(err)
		return
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Error(err)
		return
	}
	dataSet := dbtool.GenerateDataSet(1000)

	ckConn, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	err = ckConn.Insert(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)
	var result []*dbtool.DataInstance
	err = ckConn.QueryRows(&result, `select * from test.test_data_all where 1=?`, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !dbtool.CompareDataSet(dataSet, result, true) {
		t.Fatal("data is not consistent")
	}
}

func Test_ckConn_QueryStream(t *testing.T) {
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, queryNodeDNS))

	err := ckTool.Clean()
	if err != nil {
		t.Error(err)
		return
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Error(err)
		return
	}
	dataSet := dbtool.GenerateDataSet(1000)

	ckConn, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	err = ckConn.Insert(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)

	dataChan := make(chan *dbtool.DataInstance, 10)
	err = ckConn.QueryStream(dataChan, `select * from test.test_data_all where 1=?`, 1)
	if err != nil {
		t.Fatal(err)
	}

	var result []*dbtool.DataInstance
	for instance := range dataChan {
		copyInstance := instance
		result = append(result, copyInstance)
	}
	if !dbtool.CompareDataSet(dataSet, result, true) {
		t.Fatal("data is not consistent")
	}
}

func Test_ckConn_Exec(t *testing.T) {
	ckTool := dbtool.NewDBTestToolOnCK(sqlx.NewSqlConn(DRIVER, queryNodeDNS))

	err := ckTool.Clean()
	if err != nil {
		t.Error(err)
		return
	}
	err = ckTool.SetUp()
	if err != nil {
		t.Error(err)
		return
	}
	dataSet := dbtool.GenerateDataSet(1000)

	ckConn, err := NewCKConn(queryNodeDNS)
	if err != nil {
		t.Fatal(err)
	}
	err = ckConn.Insert(insertCK2, dataSet)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)
	err = ckConn.Exec(`alter table test.test_data delete where 1=?`, 1)
	if err != nil {
		t.Fatal(err)
	}
	cnt := struct {
		Cnt int `db:"cnt"`
	}{}
	time.Sleep(time.Second * 2)
	err = ckConn.QueryRow(&cnt, `select count() cnt from test.test_data`)
	if err != nil {
		t.Fatal(err)
	}
	if cnt.Cnt != 0 {
		fmt.Println(cnt.Cnt)
		t.Fatal(`ckConn.Exec is error ! `)
	}
}
