package config

import (
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/prometheus"
)

type subscriberConf struct {
	Brokers      []string `json:"Brokers"`
	Topic        string
	Group        string
	SliceSize    int
	WorkerNum    int `json:",default=32"`
	TimerPeriod  int
	ThrottleSize int `json:",default=209715200"`
}

type Config struct {
	Log  logx.LogConf
	Etcd struct {
		Hosts []string
		Key   string
	}
	// Etcd etcd.EtcdConf //etcd的配置。每个rtu节点的Etcd.Key不一样，按照`/hera/rtu/01/`格式顺序排列

	Redis struct {
		Host string
		Type string
		Pass string
	}
	BatchSize int

	Prometheus prometheus.Config

	Kafka subscriberConf `json:"kafka"`
}
