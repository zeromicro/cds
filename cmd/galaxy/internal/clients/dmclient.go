package clients

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tal-tech/cds/cmd/dm/cmd/sync/config"
	"github.com/tal-tech/cds/pkg/strx"
	"github.com/tal-tech/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type DmClient struct {
	client *clientv3.Client
}

func NewDmClient(cli *clientv3.Client) *DmClient {
	return &DmClient{client: cli}
}

// StartJob 启动一个任务
func (d *DmClient) StartJobs(job []config.Job) error {
	for _, v := range job {
		if v.ID == "" {
			return errors.New("Job ID is empty")
		}
		v.CreateTime = time.Now()
	}

	b, e := json.Marshal(job)
	if e != nil {
		logx.Error(e)
		return e
	}
	encrypt := strx.Encrypt(b)
	_, e = d.client.Put(context.TODO(), KeyJob, encrypt)
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}

// StartJob 启动一个任务
func (d *DmClient) StartJob(job config.Job) error {
	if job.ID == "" {
		return errors.New("Job ID is empty")
	}
	job.CreateTime = time.Now()

	b, e := json.Marshal(job)
	if e != nil {
		logx.Error(e)
		return e
	}
	encrypt := strx.Encrypt(b)
	_, e = d.client.Put(context.TODO(), KeyJob, encrypt)
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}

func (d *DmClient) StopJob(jobId string) error {
	b, e := json.Marshal(config.Job{
		ID:         jobId,
		CreateTime: time.Now(),
	})
	if e != nil {
		logx.Error(e)
		return e
	}

	_, e = d.client.Put(context.TODO(), KeyStopJob+jobId, string(b))
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}

func (d *DmClient) Status(jobId string) (*config.Status, error) {
	rp, e := d.client.Get(context.TODO(), KeyStatus+jobId)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	if len(rp.Kvs) == 0 {
		return nil, errors.New("DM任务未找到:" + jobId)
	}
	status := config.Status{}
	e = json.Unmarshal(rp.Kvs[0].Value, &status)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return &status, nil
}

func (d *DmClient) Delete(jobId string) (int64, error) {
	rp, e := d.client.Delete(context.TODO(), KeyStatus+jobId)
	if e != nil {
		logx.Error(e)
		return 0, e
	}
	return rp.Deleted, nil
}

func (d *DmClient) DeleteAll() (int64, error) {
	rp, e := d.client.Delete(context.TODO(), KeyStatus, clientv3.WithPrefix())
	if e != nil {
		logx.Error(e)
		return 0, e
	}
	return rp.Deleted, nil
}

func (d *DmClient) All() ([]*config.Status, error) {
	rp, e := d.client.Get(context.TODO(), KeyStatus, clientv3.WithPrefix())
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	out := []*config.Status{}
	for _, kv := range rp.Kvs {
		status := config.Status{}
		e = json.Unmarshal(kv.Value, &status)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		out = append(out, &status)
	}
	return out, nil
}
