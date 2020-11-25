package config

import (
	"github.com/tal-tech/cds/tube"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/prometheus"
)

type Config struct {
	Log  logx.LogConf
	Etcd struct {
		Hosts []string
		Key   string
	}
	//Etcd etcd.EtcdConf //etcd的配置。每个rtu节点的Etcd.Key不一样，按照`/hera/rtu/01/`格式顺序排列

	Redis struct {
		Host string
		Type string
		Pass string
	}
	BatchSize int

	Prometheus prometheus.Config

	Kafka tube.SubscriberConf `json:"kafka"`
}
