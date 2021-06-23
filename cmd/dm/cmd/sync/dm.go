package main

import (
	"context"
	"encoding/json"
	"flag"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tal-tech/cds/cmd/dm/cmd/sync/config"
	"github.com/tal-tech/cds/cmd/dm/module"
	"github.com/tal-tech/cds/pkg/strx"
	"github.com/tal-tech/go-zero/core/conf"
	"github.com/tal-tech/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var (
	f               = flag.String("f", "etc/dm.json", "The config file")
	c               config.Config
	cli             *clientv3.Client
	taskQueue       module.TaskQueue      // 任务队列
	controllerMap   map[string]*chan bool // 运行任务的控制开关
	countManager    chan int              // 运行任务的数量锁
	manager         *module.Manager       // 管理者
	prometheusOnOff chan bool             // prometheus开关
	dmPrometheus    *module.DmPrometheus
	session         *concurrency.Session // etcd锁会话
	statueHelper    *module.StatusHelper // 状态修改器
)

func main() {
	Init()
	defer func() {
		if err := session.Close(); err != nil {
			logx.Error(err)
		}
	}()

	defer func() {
		if err := cli.Close(); err != nil {
			logx.Error(err)
		}
	}()

	go dmPrometheus.Run()
	defer dmPrometheus.Stop()

	go manager.Consume()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		func() {
			timeout, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelFunc()
			response, err := cli.Grant(timeout, 1)
			if err != nil {
				logx.Error(err)
				return
			}
			session, err := concurrency.NewSession(cli, concurrency.WithLease(response.ID))
			if err != nil {
				logx.Error(err)
				return
			}
			defer session.Close()
			mutex := concurrency.NewMutex(session, "/lock/dm")
			err = mutex.Lock(timeout)
			if err != nil {
				logx.Error(err)
			}
			defer func() {
				if err := mutex.Unlock(timeout); err != nil {
					logx.Error(err)
				}
			}()
			// obtain all jobs
			resp, err := cli.Get(context.TODO(), c.Etcd.Key, clientv3.WithPrefix())
			if err != nil {
				logx.Error(err)
				return
			}
			for _, v := range resp.Kvs {
				var jobs []config.Job
				b, err := strx.Decrypt(v.Value)
				if err != nil {
					logx.Error(err)
					continue
				}
				err = json.Unmarshal(b, &jobs)
				if err != nil {
					logx.Error(err)
				}
				for i := 0; i < len(jobs); i++ {
					if err := manager.HandleJob(&jobs[i], mutex); err != nil {
						logx.Error(err)
					}
				}
			}
		}()
	}
}

func Init() {
	flag.Parse()

	c = config.Config{}
	err := conf.LoadConfig(*f, &c)
	if err != nil {
		logx.Error(err)
		return
	}

	err = logx.SetUp(c.Log)
	if err != nil {
		logx.Error(err)
		return
	}

	cli, err = clientv3.New(clientv3.Config{
		Endpoints: c.Etcd.Hosts,
	})
	if err != nil {
		logx.Error(err)
		return
	}

	taskQueue = module.NewTaskQueue()
	controllerMap = make(map[string]*chan bool)
	countManager = make(chan int, c.MaxParallelJobCount)
	statueHelper = module.NewStatusHelper(cli)
	manager = module.NewManager(controllerMap, &taskQueue, &c, &countManager, statueHelper)
	prometheusOnOff = make(chan bool)
	dmPrometheus = module.NewDmPrometheus(&c.Prometheus, prometheusOnOff)
}
