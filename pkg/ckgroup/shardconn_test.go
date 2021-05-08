package ckgroup

import (
	"testing"
)

func Test_shardConn_ExecAuto(t *testing.T) {
	{
		ck1 := &fakeCKConn{isFail: true, host: `1`}
		ck2 := &fakeCKConn{isFail: true, host: `2`}
		ck3 := &fakeCKConn{isFail: false, host: `3`}
		ck4 := &fakeCKConn{isFail: true, host: `4`}

		shard := shardConn{AllConn: []CKConn{ck1, ck2, ck3, ck4}}
		if shard.AlterAuto(``, nil) != nil {
			t.Fatal(`should not be error`)
		}
	}

	{
		ck1 := &fakeCKConn{isFail: true, host: `1`}
		ck2 := &fakeCKConn{isFail: true, host: `2`}
		ck3 := &fakeCKConn{isFail: true, host: `3`}
		ck4 := &fakeCKConn{isFail: true, host: `4`}

		shard := shardConn{AllConn: []CKConn{ck1, ck2, ck3, ck4}}
		if shard.AlterAuto(``, nil) == nil {
			t.Fatal(`should go error`)
		}
	}
}

func Test_shardConn_InsertAuto(t *testing.T) {
	{
		ck1 := &fakeCKConn{isFail: true, host: `1`}
		ck2 := &fakeCKConn{isFail: true, host: `2`}
		ck3 := &fakeCKConn{isFail: false, host: `3`}
		ck4 := &fakeCKConn{isFail: true, host: `4`}

		shard := shardConn{AllConn: []CKConn{ck1, ck2, ck3, ck4}}
		if shard.InsertAuto(``, nil) != nil {
			t.Fatal(`should not be error`)
		}
	}

	{
		ck1 := &fakeCKConn{isFail: true, host: `1`}
		ck2 := &fakeCKConn{isFail: true, host: `2`}
		ck3 := &fakeCKConn{isFail: true, host: `3`}
		ck4 := &fakeCKConn{isFail: true, host: `4`}

		shard := shardConn{AllConn: []CKConn{ck1, ck2, ck3, ck4}}
		if shard.InsertAuto(``, nil) == nil {
			t.Fatal(`should go error`)
		}
	}
}
