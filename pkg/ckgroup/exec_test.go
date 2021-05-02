package ckgroup

import (
	"database/sql"
	"errors"
	"testing"
)

func Test_dbGroup_ExecSerialAll(t *testing.T) {
	{
		shard1 := &shardConn{AllConn: []CKConn{&fakeCKConn{true, `1`}}}
		shard2 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `2`}}}
		shard3 := &shardConn{AllConn: []CKConn{&fakeCKConn{true, `3`}}}
		shard4 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `4`}}}
		group := dbGroup{ShardNodes: []ShardConn{shard1, shard2, shard3, shard4}}
		errs, _ := group.ExecSerialAll(false, ``, ``)
		if len(errs) != 1 {
			t.Fatal("length error")
		}
		if errs[0].Conn.GetHost() != `1` {
			t.Fatal("host error")
		}
	}

	{
		shard1 := &shardConn{AllConn: []CKConn{&fakeCKConn{true, `1`}}}
		shard2 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `2`}}}
		shard3 := &shardConn{AllConn: []CKConn{&fakeCKConn{true, `3`}}}
		shard4 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `4`}}}
		group := dbGroup{ShardNodes: []ShardConn{shard1, shard2, shard3, shard4}}
		errs, _ := group.ExecSerialAll(true, ``, ``)
		if len(errs) != 2 {
			t.Fatal("length error")
		}
		if errs[0].Conn.GetHost() != `1` || errs[1].Conn.GetHost() != `3` {
			t.Fatal("host error")
		}
	}

	{
		shard1 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `1`}}}
		shard2 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `2`}}}
		shard3 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `3`}}}
		shard4 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `4`}}}
		group := dbGroup{ShardNodes: []ShardConn{shard1, shard2, shard3, shard4}}
		errs, _ := group.ExecSerialAll(true, ``, ``)
		if len(errs) != 0 {
			t.Fatal("length error")
		}
	}
}

type fakeCKConn struct {
	isFail bool
	host   string
}

func (conn *fakeCKConn) GetHost() string {
	return conn.host
}

func (conn *fakeCKConn) GetUser() string {
	panic("implement me")
}

func (conn *fakeCKConn) GetRawConn() *sql.DB {
	panic("implement me")
}

func (conn *fakeCKConn) Exec(query string, args ...interface{}) error {
	if conn.isFail {
		return errors.New("fake error")
	}
	return nil
}

func (conn *fakeCKConn) QueryRowNoType(query string, args ...interface{}) (map[string]interface{}, error) {
	panic("implement me")
}

func (conn *fakeCKConn) QueryRowsNoType(query string, args ...interface{}) ([]map[string]interface{}, error) {
	panic("implement me")
}

func (conn *fakeCKConn) QueryRow(v interface{}, query string, args ...interface{}) error {
	panic("implement me")
}

func (conn *fakeCKConn) QueryRows(v interface{}, query string, args ...interface{}) error {
	panic("implement me")
}

func (conn *fakeCKConn) QueryStream(chanData interface{}, query string, args ...interface{}) error {
	panic("implement me")
}

func (conn *fakeCKConn) Insert(query string, sliceData interface{}) error {
	if conn.isFail {
		return errors.New("fake error")
	}
	return nil
}

func Test_dbGroup_ExecParallelAll(t *testing.T) {
	{
		shard1 := &shardConn{AllConn: []CKConn{&fakeCKConn{true, `1`}}}
		shard2 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `2`}}}
		shard3 := &shardConn{AllConn: []CKConn{&fakeCKConn{true, `3`}}}
		shard4 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `4`}}}
		group := dbGroup{ShardNodes: []ShardConn{shard1, shard2, shard3, shard4}}
		errs, _ := group.ExecParallelAll(``, ``)
		if len(errs) != 2 {
			t.Fatal("length error")
		}
		str := errs[0].Conn.GetHost() + errs[1].Conn.GetHost()
		if !(str == `13` || str == `31`) {
			t.Fatal("host error")
		}
	}

	{
		shard1 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `1`}}}
		shard2 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `2`}}}
		shard3 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `3`}}}
		shard4 := &shardConn{AllConn: []CKConn{&fakeCKConn{false, `4`}}}
		group := dbGroup{ShardNodes: []ShardConn{shard1, shard2, shard3, shard4}}
		errs, _ := group.ExecParallelAll(``, ``)
		if len(errs) != 0 {
			t.Fatal("length error")
		}
	}
}

func Test_dbGroup_AlterAuto(t *testing.T) {
	{
		group := dbGroup{
			ShardNodes: []ShardConn{&fakeShardConn{true}, &fakeShardConn{true}, &fakeShardConn{true}},
		}
		errs, _ := group.AlterAuto(`alter`, ``)
		if len(errs) != 3 {
			t.Fatal("length error")
		}
		for i, err := range errs {
			if err.ShardIndex != i+1 {
				t.Fatal(`shard index error`)
			}
		}
	}

	{
		group := dbGroup{
			ShardNodes: []ShardConn{&fakeShardConn{true}, &fakeShardConn{false}, &fakeShardConn{true}},
		}
		errs, _ := group.AlterAuto(`alter`, ``)
		if len(errs) != 2 {
			t.Fatal("length error")
		}
		for i, err := range errs {
			if i == 1 {
				continue
			}
			if err.ShardIndex != i+1 {
				t.Fatal(`shard index error`)
			}
		}
	}

	{
		group := dbGroup{
			ShardNodes: []ShardConn{&fakeShardConn{false}, &fakeShardConn{false}, &fakeShardConn{false}},
		}
		errs, _ := group.AlterAuto(`alter`, ``)
		if len(errs) != 0 {
			t.Fatal("length error")
		}
	}

}

func Test_isAlterSQL(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{args: args{` 	 alter table user drop column a`}, want: true},
		{args: args{`alter table user drop column a`}, want: true},
		{args: args{`	Alter table user drop column a`}, want: true},
		{args: args{` alter table user drop column a`}, want: true},
		{args: args{`  table user drop column a`}, want: false},
		{args: args{`alter`}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAlterSQL(tt.args.sql); got != tt.want {
				t.Errorf("isAlterSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
