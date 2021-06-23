package handle

import (
	"log"
	"sync"
	"time"

	"github.com/tal-tech/cds/cmd/rtu/cmd/sync/config"

	"github.com/tal-tech/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	conf     *config.Config
	taskList *taskRecords
)

type taskRecords struct {
	m map[string]*runEngine
	sync.RWMutex
}

func (t *taskRecords) cleanDeadJob() {
	t.Lock()
	defer t.Unlock()
	for key, job := range t.m {
		if !job.isAlive() {
			logx.Infof("[clean] (%s) is dead, clean", job.conf.Kafka.Topic)
			delete(t.m, key)
		}
	}
}
func (t *taskRecords) removeLast() {
	t.Lock()
	defer t.Unlock()
	for k, job := range t.m {
		job.doStop()
		delete(t.m, k)
	}
}

func (t *taskRecords) removeAndStop(key string) {
	t.Lock()
	defer t.Unlock()
	job := t.m[key]
	if job != nil {
		logx.Infof("[%s], to_stop", t.m[key].conf.Source.Topic)
		job.doStop()
		logx.Infof("[%s], stop_ok", t.m[key].conf.Source.Topic)
	}
	delete(t.m, key)
}

func (t *taskRecords) getAllKey() []string {
	t.RLock()
	defer t.RUnlock()
	l := make([]string, 0, len(t.m))
	for k := range t.m {
		l = append(l, k)
	}
	return l
}

func (t *taskRecords) checkAndAdd(job *config.Job) (bool, *runEngine) {
	t.RLock()
	if _, ok := t.m[job.ID]; ok {
		defer t.RUnlock()
		return true, nil
	}
	t.RUnlock()
	t.Lock()
	if _, ok := t.m[job.ID]; ok {
		defer t.Unlock()
		return true, nil
	}
	defer t.Unlock()
	t.m[job.ID] = newRunEngine(job)

	return false, t.m[job.ID]
}

// SetConfig 设置 conf 到 handle 全局变量
func SetConfig(config *config.Config) {
	if config == nil {
		logx.Error("conf can't be nil")
		log.Fatal("conf can't be nil")
	}
	conf = config
}

func newEtcdCli() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: conf.Etcd.Hosts,
	})
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	return cli, err
}

// Run 运行核心逻辑
func Run(exit chan struct{}, wg *sync.WaitGroup) {
	taskList = &taskRecords{
		m: make(map[string]*runEngine, 6),
	}

	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for range tick.C {
			taskList.cleanDeadJob()
		}
	}()

	cli, err := newEtcdCli()
	if err != nil {
		logx.Error(err)
		return
	}
	go updateRunStatus(cli)
	defer func() {
		err := cli.Close()
		if err != nil {
			logx.Error(err)
		}
	}()

	checkNewTaskCmd(exit, wg, cli)
}

func checkNewTaskCmd(exit chan struct{}, wg *sync.WaitGroup, cli *clientv3.Client) {
	logx.Info("watching ...")
	withDistribetuLock(cli, wg, exit)

	tick := time.NewTicker(time.Second * 3)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			withDistribetuLock(cli, wg, exit)
		case <-exit:
			return
		}
	}
}
