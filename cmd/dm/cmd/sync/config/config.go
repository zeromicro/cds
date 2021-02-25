package config

import (
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/prometheus"
)

type (
	Config struct {
		Log                 logx.LogConf
		Etcd                EtcdConf //etcd的配置。每个dm节点的Etcd.Key不一样，按照`/hera/dm/01/`格式顺序排列
		MaxParallelJobCount int      //最大并行任务数
		MongoBatchSize      int      // Mongo 批量提交大小
		Prometheus          prometheus.Config
	}
	EtcdConf struct {
		Hosts []string
		Key   string
	}
)
