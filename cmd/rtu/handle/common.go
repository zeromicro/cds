package handle

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"

	"github.com/tal-tech/cds/cmd/rtu/cmd/sync/config"
	"github.com/tal-tech/cds/cmd/rtu/monitor"
	"github.com/tal-tech/cds/pkg/ckgroup"
)

// prometheus vec
type monitorVec struct {
	kafka   *monitor.KafkaVec
	db      *monitor.DatabaseVec
	limiter *monitor.DatabaseVec
	runtime *monitor.GaugeVec
}

const (
	TIMEOUT = 60
)

var (
	vecs *monitorVec

	CkgroupStore ckgroupStore

	isReset atomic.Bool
)

type ckgroupStore struct {
	m map[string]ckgroup.DBGroup
	sync.RWMutex
}

func init() {
	isReset.Store(false)
	vecs = &monitorVec{
		db:      monitor.NewDatabaseVec("rtu", "db", "sync"),
		kafka:   monitor.NewKafkaVec("rtu", "kafka", "sync"),
		limiter: monitor.NewDatabaseVec("rtu", "limiter", "sync"),
		runtime: monitor.NewGaugerVec("rtu", "runtime", "sync", []string{"db", "table", "category"}...),
	}
	CkgroupStore = ckgroupStore{
		m: make(map[string]ckgroup.DBGroup, 4),
	}
}

func (cks *ckgroupStore) checkAndAdd(conf *config.Job) ckgroup.DBGroup {
	res, err := json.Marshal(conf.Target.Shards)
	if err != nil {
		logx.Error(err)
	}
	cks.RLock()
	if v, ok := cks.m[string(res)]; ok {
		defer cks.RUnlock()
		return v
	}
	cks.RUnlock()
	cks.Lock()
	if v, ok := cks.m[string(res)]; ok {
		defer cks.Unlock()
		return v
	}

	cfg := formatChCfg(conf)

Init:
	ch, err := ckgroup.NewCKGroup(cfg)
	if err != nil {
		logx.Errorf("init ckGroup Err. err: %s", err)
		time.Sleep(10 * time.Second)
		goto Init
	}
	cks.m[string(res)] = ch
	ch.KeepAlive(10)

	defer cks.Unlock()
	return ch
}

func setEtcdStatus(id, key, content, status string, client *clientv3.Client) error {
	val := config.Status{
		ID:          id,
		Status:      status,
		Information: content,
		UpdateTime:  time.Now(),
	}
	v, err := json.Marshal(&val)
	if err != nil {
		logx.Error(err)
	}
	k := key + id
	_, err = client.Put(context.Background(), k, string(v))
	if err != nil {
		logx.Error(err)
	}
	return err
}
