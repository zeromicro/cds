//go:build integration
// +build integration

package ckgroup

import (
	"testing"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/zeromicro/cds/pkg/ckgroup/config"
)

const (
	insertCK  = "insert into test.test_data (pk,int_value,float_value,double_value,char_value,varchar_value,time_value) values(?,?,?,?,?,?,?)"
	insertCK2 = "insert into test.test_data (pk,int_value,float_value,double_value,char_value,varchar_value,time_value) values(#{pk},#{int_value},#{float_value},#{double_value},#{char_value},#{varchar_value},#{time_value})"
)

var c = config.Config{ShardGroups: []config.ShardGroupConfig{
	{ShardNode: "tcp://localhost:9000", ReplicaNodes: []string{"tcp://localhost:9001"}},
	{ShardNode: "tcp://localhost:9002", ReplicaNodes: []string{"tcp://localhost:9003"}},
}, QueryNode: queryNodeDNS}

func TestDBGroup_ExecAll(t *testing.T) {
	group := MustCKGroup(c)
	query := `alter table test.test_data delete where 1<2`
	e := group.ExecAll(query, nil)
	if e != nil {
		t.Log(e)
		return
	}
}
