package config

import (
	"github.com/tal-tech/cds/tube"
	"time"
)

// Job 任务配置
// 一般从etcd中拉取，按照这个配置来建立新的任务
// etcd中key的格式：/hera/rtu/job
type Job struct {
	ID string //任务id

	Source struct { //数据源
		Type     string //类型 mysql,mongodb
		Topic    string //需要监听的topic
		Dsn      string //连接串
		Table    string //表名
		QueryKey string //查询键
	}
	Target struct { //目标库
		Type    string     //类型 clickhouse
		Shards  [][]string //集群连接串列表，如 [3][2]string代表3个shard，每个shard双备份
		ChProxy string     //chproxy的连接串
		Table   string     //表名
		Db      string
	}

	Kafka tube.SubscriberConf

	CreateTime time.Time //任务创建时间戳
}

// StopJob 任务停止指令
// 从etcd中获取，按照任务id来删除相应任务
// etcd中的key: /hera/rtu/stop-job/{{任务id,如8b8x7g3c}}
type StopJob struct {
	ID         string    //任务id
	CreateTime time.Time //指令发送时间戳
}

// Status 任务状态
// 由rtu将任务运行状态回写至etcd
// Etcd中key的格式为：/hera/rtu/status/{{Job编号，如8172bx9873h4bc98g212}}
type Status struct {
	ID          string    //任务id
	Status      string    //状态 pending:排队 running:运行 pause:暂停 error:出错 finish:任务完成 stopped:任务被停止
	Information string    //状态详情
	UpdateTime  time.Time //状态更新时间
}

const (
	//type
	TYPE_MYSQL      = "mysql"
	TYPE_MONGODB    = "mongodb"
	TYPE_CLICKHOUSE = "clickhouse"
	//status
	STATUS_PENDING  = "pending"
	STATUS_RUNNING  = "running"
	STATUS_PAUSE    = "pause"
	STATUS_ERROR    = "error"
	STATUS_FINISHED = "finished"
	STATUS_STOPPED  = "stopped"
)
