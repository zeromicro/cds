package clickhousex

import (
	"errors"
	"testing"
)

var shards = [][]string{
	{"1", "2"},
	{"3", "4"},
	{"5", "6"},
}

func TestExecClusterAllReplicas(t *testing.T) {
	i := 0
	e := ExecClusterAllReplicas(shards, 0, func(dsn string) error {
		i++
		return nil
	})
	if e != nil {
		t.Error(e)
		return
	}
	if i != 6 {
		t.Error("i is not 6 , but ", i)
		return
	}

	i = 0
	e = ExecClusterAllReplicas(shards, 0, func(dsn string) error {
		i++
		if dsn == "3" {
			return errors.New("error")
		}
		return nil
	})
	if e == nil {
		t.Error(errors.New("error is nil"))
		return
	}
	if i != 3 {
		t.Error("i is not 5 , but ", i)
		return
	}
}

func TestExecClusterEachShards(t *testing.T) {
	i := 0
	e := ExecClusterEachShards(shards, 0, func(dsn string) error {
		i++
		switch dsn {
		case "1", "3", "6":
			return errors.New("fail point")
		default:
			return nil
		}
	})
	if e != nil {
		t.Error(e)
		return
	}
	if i != 5 {
		t.Error("i is not 5 , but ", i)
		return
	}
}

func TestExecClusterAnyShard(t *testing.T) {
	i := 0
	e := ExecClusterAnyShard(shards, 0, func(dsn string) error {
		i++
		switch dsn {
		case "1", "2", "3", "5", "6":
			return errors.New("fail point:" + dsn)
		default:
			return nil
		}
	})
	if e != nil {
		t.Error(e)
		return
	}
	if i != 4 {
		t.Error("i is not 4 , but ", i)
		return
	}
}

func TestExecClusterEachShardsAll(t *testing.T) {
	i := 0
	e := ExecClusterEachShardsAll(shards, 0, func(dsn string) error {
		i++
		switch dsn {
		case "1", "3", "6":
			return errors.New("fail point")
		default:
			return nil
		}
	})
	if e != nil {
		t.Error(e)
		return
	}
	if i != 6 {
		t.Error("i is not 6 , but ", i)
		return
	}
}
