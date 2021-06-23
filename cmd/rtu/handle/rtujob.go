package handle

import (
	"math/rand" // #nosec
	"sync"
	"sync/atomic"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tal-tech/cds/cmd/rtu/cmd/sync/config"
	"github.com/tal-tech/cds/cmd/rtu/model"
	"github.com/tal-tech/cds/cmd/rtu/monitor"
	"github.com/tal-tech/cds/pkg/ckgroup"
	groupcfg "github.com/tal-tech/cds/pkg/ckgroup/config"
)

type runEngine struct {
	clickhouseTable *model.SQLTable
	chInsertNode    ckgroup.DBGroup

	insert *insertEngine
	parse  *parseEngine
	input  *inputEngine

	mapPool *model.MapPool

	conf *config.Job

	closeChan chan struct{}
	isClosed  *int64

	// 是否stop
	isStoped bool

	labels     *runLabel
	monitorVec *monitorVec

	etcd *clientv3.Client // etcd

	lastRunTime int64
}

// prometheus labels
type runLabel struct {
	insertOkLabel       *monitor.DatabaseLabels
	insertFailedLabel   *monitor.DatabaseLabels
	insertNonBlockLabel *monitor.DatabaseLabels
	insertBlockLabel    *monitor.DatabaseLabels
	insertCntLabel      *monitor.DatabaseLabels

	queryOkLabel       *monitor.DatabaseLabels
	queryFailedLabel   *monitor.DatabaseLabels
	queryNonBlockLabel *monitor.DatabaseLabels
	queryBlockLabel    *monitor.DatabaseLabels

	msgCntLabel *monitor.KafkaLabels
	msgOkLable  *monitor.KafkaLabels

	batchSizeLable   *monitor.GaugeLabels
	batchLengthLable *monitor.GaugeLabels
}

func newRunEngine(cfg *config.Job) *runEngine {
	processExitCh := make(chan struct{}, 1)

	labels := &runLabel{
		insertOkLabel:       &monitor.DatabaseLabels{Status: "ok", Table: cfg.Target.Table, Action: "insert"},
		insertFailedLabel:   &monitor.DatabaseLabels{Status: "failed", Table: cfg.Target.Table, Action: "insert"},
		insertNonBlockLabel: &monitor.DatabaseLabels{Status: "block", Table: cfg.Target.Table, Action: "insert"},
		insertBlockLabel:    &monitor.DatabaseLabels{Status: "pass", Table: cfg.Target.Table, Action: "insert"},
		insertCntLabel:      &monitor.DatabaseLabels{Status: "cnt", Table: cfg.Target.Table, Action: "insert"},

		queryOkLabel:       &monitor.DatabaseLabels{Status: "ok", Table: cfg.Target.Table, Action: "query"},
		queryFailedLabel:   &monitor.DatabaseLabels{Status: "failed", Table: cfg.Target.Table, Action: "query"},
		queryNonBlockLabel: &monitor.DatabaseLabels{Status: "block", Table: cfg.Target.Table, Action: "query"},
		queryBlockLabel:    &monitor.DatabaseLabels{Status: "pass", Table: cfg.Target.Table, Action: "query"},

		msgCntLabel: &monitor.KafkaLabels{Topic: cfg.Kafka.Topic, Status: "ok"},
		msgOkLable:  &monitor.KafkaLabels{Topic: cfg.Kafka.Topic, Status: "nextBatch"},

		batchSizeLable:   &monitor.GaugeLabels{Db: cfg.Target.Db, Table: cfg.Target.Table, Category: "size"},
		batchLengthLable: &monitor.GaugeLabels{Db: cfg.Target.Db, Table: cfg.Target.Table, Category: "length"},
	}

	return &runEngine{
		conf:       cfg,
		closeChan:  processExitCh,
		isClosed:   new(int64),
		labels:     labels,
		monitorVec: vecs,
	}
}

func (rengine *runEngine) Start(exit chan struct{}, wg *sync.WaitGroup) {
	rengine.isStoped = false
	*rengine.isClosed = 0
	// todo keep alive
	etcd, err := newEtcdCli()
	if err != nil {
		logx.Error(err)
		return
	}
	rengine.etcd = etcd
	err = rengine.setEtcdRunStatus(config.STATUS_RUNNING, "init")
	if err != nil {
		logx.Error(err)
		etcd.Close()
		return
	}

	go rengine.doStart(exit, wg)
}

func (rengine *runEngine) Restart(exit chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	rengine.doStop()
	for {
		time.Sleep(1 * time.Second)
		if rengine.isStoped {
			break
		}
	}
	rengine.Start(exit, wg)
}

// 运行 主函数
func (rengine *runEngine) doStart(exit chan struct{}, wg *sync.WaitGroup) {
	if conf == nil {
		logx.Error("conf init required")
		_ = rengine.setEtcdRunStatus(config.STATUS_ERROR, "conf err")
		return
	}

	defer rengine.etcd.Close()
	logx.Infof("[%s] start", rengine.conf.Source.Topic)

	defer wg.Done()

	rengine.init()
	rengine.start(exit)

	select {
	case <-exit:
	case <-rengine.closeChan:
	}

	rengine.stop()

	err := rengine.setEtcdRunStatus(config.STATUS_STOPPED, "")
	if err != nil {
		logx.Error(err)
	}

	rengine.isStoped = true
}

func (rengine *runEngine) init() {
	rengine.mapPool = model.NewMapPool()
	rengine.input = newInput(rengine)

	err := rengine.initClickhouse()
	if err != nil {
		logx.Errorf("init err, exit now. detail: %v", err)
		_ = rengine.setEtcdRunStatus(config.STATUS_ERROR, "init clickhouse err")
		return
	}

	rengine.insert = newInsertEngine(rengine)
	rengine.parse = newParseEngine(rengine.conf.Source.Type, rengine)
}

// 启动
func (rengine *runEngine) start(exit chan struct{}) {
	rengine.insert.wg.Add(1)
	go rengine.insert.start()

	rengine.parse.wg.Add(1)
	go rengine.parse.start()

	rengine.input.wg.Add(1)
	go rengine.input.start()

	go rengine.refreshStatus(exit)
	go rengine.autoResetTimer()
}

// 停止并回收
func (rengine *runEngine) stop() {
	rengine.input.stop()
	logx.Infof("[%s] input closed", rengine.conf.Source.Topic)
	rengine.parse.stop()
	logx.Infof("[%s] parse closed", rengine.conf.Source.Topic)
	rengine.insert.stop()
	logx.Infof("[%s] insert closed", rengine.conf.Source.Topic)
}

func (rengine *runEngine) doStop() {
	ok := atomic.CompareAndSwapInt64(rengine.isClosed, 0, 1)
	if ok {
		close(rengine.closeChan)
	}
	atomic.StoreInt64(rengine.isClosed, 2)
}

// 刷新状态
func (rengine *runEngine) refreshStatus(exit chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rengine.lastRunTime = time.Now().Unix()
			err := rengine.setEtcdRunStatus(config.STATUS_RUNNING, "")
			if err != nil {
				logx.Error(err)
			}
		case <-exit:
			return
		case <-rengine.closeChan:
			return
		}
	}
}

func (rengine *runEngine) setEtcdRunStatus(status, content string) error {
	return setEtcdStatus(rengine.conf.ID, "/hera/rtu/status/", content, status, rengine.etcd)
}

func (rengine *runEngine) isAlive() bool {
	return time.Now().Unix()-rengine.lastRunTime < 60
}

func (rengine *runEngine) autoResetTimer() {
	restartTime := 21600 + rand.Int63n(2000) // #nosec
	now := time.Now().Unix()
	logx.Infof("[autoReset] %s will reset at %s", rengine.conf.Kafka.Topic, time.Unix(now+restartTime, 0))
	tick := time.NewTicker(time.Second * time.Duration(restartTime))
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			rengine.doStop()
			logx.Infof("[autoReset] %s reset now", rengine.conf.Kafka.Topic)

			return
		case <-rengine.closeChan:
			return
		}
	}
}

// 初始化ch，和表结构
func (rengine *runEngine) initClickhouse() error {

	rengine.chInsertNode = CkgroupStore.checkAndAdd(rengine.conf)
RETRYREFRESH:
	err := rengine.refreshClickhouseTable()
	if err != nil {
		logx.Errorf("[%s] err: %s", rengine.conf.Kafka.Topic, err)
		time.Sleep(5 * time.Second)
		goto RETRYREFRESH
	}
	return nil
}

func formatChCfg(conf *config.Job) groupcfg.Config {
	shardCfgs := make([]groupcfg.ShardGroupConfig, 0, len(conf.Target.Shards))
	for _, i := range conf.Target.Shards {
		cfg := groupcfg.ShardGroupConfig{ReplicaNodes: make([]string, 0, len(i)-1)}
		for index, addr := range i {
			if index == 0 {
				cfg.ShardNode = addr
			} else {
				cfg.ReplicaNodes = append(cfg.ReplicaNodes, addr)
			}
		}
		shardCfgs = append(shardCfgs, cfg)
	}
	return groupcfg.Config{
		ShardGroups: shardCfgs,
		QueryNode:   shardCfgs[0].ShardNode,
	}
}

func (rengine *runEngine) refreshClickhouseTable() error {
	var err error
	rengine.clickhouseTable, err = GetClickhouseTableColumn(
		rengine.chInsertNode.GetQueryNode().GetRawConn(),
		rengine.conf.Target.Db,
		rengine.conf.Target.Table,
		rengine.conf.Source.QueryKey,
		rengine.conf.Source.Type,
	)
	if err != nil {
		logx.Error(err)
		return err
	}
	return nil
}
