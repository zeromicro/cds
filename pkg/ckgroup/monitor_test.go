package ckgroup

import (
	"testing"
	"time"
)

func Test_hostname(t *testing.T) {
	label := getInsertLabel(`db`, `table`, `host`, `1`)
	insertDuHis.With(label).Observe(float64(time.Since(time.Now()).Milliseconds()))
	insertBatchSizeGa.With(label).Set(float64(len([]interface{}{111})))
	insertBatchSizeHis.With(label).Observe(float64(len([]interface{}{111})))
}
