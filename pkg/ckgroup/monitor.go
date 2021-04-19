package ckgroup

import (
	"github.com/prometheus/client_golang/prometheus"
	"os"
)

const (
	namespace     = `ckgroup`
	hostNameLabel = `host_name`
)

const (
	insertDBLab      = `database`
	insertTableLab   = `table`
	insertHostLab    = `ck_host`
	insertSuccessLab = `is_success`
)

var (
	insertCntHis *prometheus.HistogramVec
	insertDuHis  *prometheus.HistogramVec
	hostName     = hostname()
)

func init() {
	insertCounterOps := prometheus.HistogramOpts{
		Namespace:   namespace,
		Name:        "insert",
		Help:        `插入数量统计`,
		ConstLabels: map[string]string{hostNameLabel: hostName},
		Buckets:     []float64{500, 2000, 5000, 10000, 25000, 50000, 70000, 100000},
	}
	insertDurationHisOps := prometheus.HistogramOpts{
		Namespace:   namespace,
		Name:        "insert_duration",
		Help:        `插入耗时,单位毫秒`,
		ConstLabels: map[string]string{hostNameLabel: hostName},
		Buckets:     []float64{10, 20, 50, 100, 300, 600, 1000, 1500, 3000},
	}
	insertCntHis = prometheus.NewHistogramVec(insertCounterOps, []string{insertDBLab, insertTableLab, insertHostLab, insertSuccessLab})
	insertDuHis = prometheus.NewHistogramVec(insertDurationHisOps, []string{insertDBLab, insertTableLab, insertHostLab, insertSuccessLab})
	prometheus.MustRegister(insertCntHis, insertDuHis)

}

func hostname() string {
	host, err := os.Hostname()
	if err != nil {
		host = `unknow`
	}
	return host
}

func getInsertLabel(db, table, ckHost, success string) prometheus.Labels {
	return prometheus.Labels{insertDBLab: db, insertTableLab: table, insertHostLab: ckHost, insertSuccessLab: success}
}
