package handle

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tal-tech/cds/cmd/rtu/cmd/sync/config"
	"github.com/tal-tech/cds/pkg/strx"
	"github.com/tal-tech/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var (
	hostname   string
	nodePrefix = "node/"
)

func init() {
	hostname, _ = os.Hostname()
}

func withDistribetuLock(client *clientv3.Client, wg *sync.WaitGroup, exit chan struct{}) {
	timeout, cancelFunc := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFunc()
	response, e := client.Grant(timeout, 10)
	if e != nil {
		logx.Error(e.Error())
		return
	}
	session, e := concurrency.NewSession(client, concurrency.WithLease(response.ID))
	if e != nil {
		logx.Error(e.Error())
		return
	}
	defer func() {
		err := session.Close()
		if err != nil {
			logx.Error(err)
		}
	}()
	mutex := concurrency.NewMutex(session, "/lock/rtu")
	e = mutex.Lock(timeout)
	if e != nil {
		logx.Error(e.Error())
		return
	}
	defer func() {
		err := mutex.Unlock(timeout)
		if err != nil {
			logx.Error(err)
		}
	}()

	createNewTask(wg, exit, client)
}

func createNewTask(wg *sync.WaitGroup, exit chan struct{}, client *clientv3.Client) {
	vals := getLatestValue(client, conf.Etcd.Key)
	if len(vals) == 0 {
		return
	}
	jobs := []*config.Job{}
	dec, err := strx.Decrypt(vals)
	if err != nil {
		logx.Error(err)
		return
	}
	err = json.Unmarshal(dec, &jobs)
	if err != nil {
		logx.Error(err)
		logx.Info(string(vals))
		return
	}
	allKey := len(taskList.getAllKey())
	jobCnt := len(jobs)
	NodeCnt := getCliListCnt(client)

	if allKey > jobCnt/NodeCnt+1 {
		logx.Infof("[rebalance] taskCnt %d, jobCnt %d, nodeCnt %d", allKey, jobCnt, NodeCnt)
		taskList.removeLast()
		return
	}

	// note for debug
	jobTopics := make([]string, 0, len(jobs))
	for _, job := range jobs {
		jobTopics = append(jobTopics, job.Source.Topic)
	}
	logx.Infof("topics in job :[%s]", strings.Join(jobTopics, ","))
	fetchJobOk := false
	jobsInList := map[string]struct{}{}

	for _, job := range jobs {
		jobsInList[job.ID] = struct{}{}
		if checkJobExists(client, job.ID, "/hera/rtu/status/") {
			logx.Infof("[%s] exists", job.Source.Topic)
			continue
		}
		// 一轮只拿一个，拿过了设置为 true
		if fetchJobOk {
			continue
		}
		job.Kafka = conf.Kafka
		job.Kafka.Topic = job.Source.Topic
		logx.Info(job.Kafka.Topic)
		// todo . from etcd
		job.Kafka.Group = job.Kafka.Topic + "_rtu"

		// 运行任务
		exists, newRunEg := taskList.checkAndAdd(job)
		if exists {
			logx.Infof("task: %s is exists, skip", job.Source.Topic)
			continue
		}
		if newRunEg == nil {
			logx.Error("new run engine is nil")
			continue
		}
		wg.Add(1)
		newRunEg.Start(exit, wg)
		fetchJobOk = true
	}
	for _, runJobID := range taskList.getAllKey() {
		if _, ok := jobsInList[runJobID]; !ok {
			taskList.removeAndStop(runJobID)
		}
	}
}

func getLatestValue(client *clientv3.Client, key string) []byte {
	resp, err := client.Get(context.TODO(), key)
	if err != nil {
		logx.Error(err)
		return nil
	}
	if len(resp.Kvs) < 1 {
		return nil
	}

	return resp.Kvs[0].Value
}

// false 不存在，true 存在
func checkJobExists(client *clientv3.Client, jobID string, prefix string) bool {
	b := getLatestValue(client, prefix+jobID)
	// logx.Infof("[debug] jobid%s result:(%s)", jobID, string(b))
	if b == nil {
		return false
	}
	// logx.Infof("exists1: %s", string(b))
	jobStatus := &config.Status{}
	err := json.Unmarshal(b, jobStatus)
	if err != nil {
		return false
	}

	if jobStatus.Status == config.STATUS_ERROR {
		return true
	}
	if time.Now().Unix()-jobStatus.UpdateTime.Unix() > TIMEOUT {
		return false
	}
	return true
}

func getCliListCnt(cli *clientv3.Client) int {
	res, err := cli.Get(context.TODO(), nodePrefix, clientv3.WithPrefix())
	if err != nil {
		logx.Error(err)
		return 1
	}
	return int(res.Count)
}

func updateRunStatus(cli *clientv3.Client) {
	for {
		lease, err := cli.Grant(context.TODO(), 65)
		if err != nil {
			logx.Error(err)
			continue
		}
		_, err = cli.Put(context.TODO(), nodePrefix+hostname, "1", clientv3.WithLease(lease.ID))
		if err != nil {
			logx.Error(err)
			continue
		}
		time.Sleep(time.Minute)
	}
}
