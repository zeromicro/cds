package ckgroup

import (
	"testing"
	"time"

	"github.com/tal-tech/cds/pkg/ckgroup/dbtesttool/dbtool"
	"golang.org/x/time/rate"
)

func TestWithGroupInsertLimiter(t *testing.T) {
	db := dbGroup{
		ShardNodes: []ShardConn{&fakeShardConn{true}},
		opt:        option{GroupInsertLimiter: rate.NewLimiter(rate.Every(time.Millisecond*500), 1)},
	}
	dataSet := dbtool.GenerateDataSet(10000)
	start := time.Now()
	for i := 1; i <= 3; i++ {
		err := db.InsertAuto(``, `pk`, dataSet)
		if err != nil {
			t.Fatal(err)
		}
	}

	if time.Since(start) <= time.Millisecond*900 {
		t.Fatal(`insert limiter did not meet expectations`)
	}
}
