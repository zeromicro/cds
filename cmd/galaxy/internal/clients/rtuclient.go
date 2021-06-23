package clients

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tal-tech/cds/cmd/rtu/cmd/sync/config"
	"github.com/tal-tech/cds/pkg/strx"
	"github.com/tal-tech/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type RtuClient struct {
	client *clientv3.Client
}

func NewRtuClient(client *clientv3.Client) *RtuClient {
	return &RtuClient{client: client}
}

func (r *RtuClient) StartJob(job config.Job) error {
	if job.ID == "" {
		return errors.New("Job ID is empty")
	}
	s, e := r.Status(job.ID)
	if e != nil {
		logx.Error(e)
		return e
	}
	if s != nil {
		switch s.Status {
		case config.STATUS_PENDING:
			return errors.New("Job is already pending")
		case config.STATUS_RUNNING:
			return errors.New("Job is already running")
		}
	}
	job.CreateTime = time.Now()

	b, e := json.Marshal(job)
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = r.client.Put(context.TODO(), r.JobKey(), string(b))
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}

func (r *RtuClient) StopJob(jobId string) error {
	s, e := r.Status(jobId)
	if e != nil {
		logx.Error(e)
		return e
	}
	if s == nil {
		return errors.New("Job " + jobId + " not exists")
	}
	b, e := json.Marshal(config.Job{
		ID:         jobId,
		CreateTime: time.Now(),
	})
	if e != nil {
		logx.Error(e)
		return e
	}

	_, e = r.client.Put(context.TODO(), r.StopJobKey(jobId), string(b))
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}

func (r *RtuClient) Status(jobId string) (*config.Status, error) {
	rp, e := r.client.Get(context.TODO(), r.StatusKey(jobId))
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	if len(rp.Kvs) == 0 {
		return nil, nil
	}
	kv := rp.Kvs[0]
	status := config.Status{}
	e = json.Unmarshal(kv.Value, &status)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return &status, nil
}

func (r *RtuClient) JobKey() string {
	return "/hera/rtu/job"
}

func (r *RtuClient) StopJobKey(jobID string) string {
	return "/hera/rtu/stop-job/" + jobID
}

func (r *RtuClient) StatusKey(jobID string) string {
	return "/hera/rtu/status/" + jobID
}

func (r *RtuClient) Delete(jobId string) (int64, error) {
	rp, e := r.client.Delete(context.TODO(), RtuKeyStatus+jobId)
	if e != nil {
		logx.Error(e)
		return 0, e
	}
	return rp.Deleted, nil
}

func (r *RtuClient) All() ([]*config.Status, error) {
	rp, e := r.client.Get(context.TODO(), RtuKeyStatus, clientv3.WithPrefix())
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	out := []*config.Status{}
	for _, kv := range rp.Kvs {
		v := config.Status{}
		e = json.Unmarshal(kv.Value, &v)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		out = append(out, &v)
	}
	return out, nil
}

func (r *RtuClient) StartJobs(job []config.Job) error {
	b, e := json.Marshal(job)
	if e != nil {
		logx.Error(e)
		return e
	}
	encrypt := strx.Encrypt(b)
	_, e = r.client.Put(context.TODO(), r.JobKey(), encrypt)
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}
