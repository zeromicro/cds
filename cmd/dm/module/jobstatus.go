package module

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tal-tech/cds/cmd/dm/cmd/sync/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	StatusPrefix = "/hera/dm/status/"

	TipStatusPending        = "Join in queue, wait for execute. My WindowPeriod is %d - %d "
	TipStatusRunning        = "I M Running ~"
	TipStatusError          = "Error Occurred : %s ."
	TipStatusFinished       = "Job Finished at %s "
	TipStatusStoppedRunning = "Manually stopped when running"
	TipStatusStoppedPending = "Manually stopped when still pending"

	StatusPendingError    = "Status is pending but could not find it in the taskQueue "
	StatusCannotStopError = "Status is %s . This status cannot be stopped "
)

type StatusHelper struct {
	Client *clientv3.Client
}

func NewStatus(id string, status string, info string, updateTime time.Time) *config.Status {
	return &config.Status{ID: id, Status: status, Information: info, UpdateTime: updateTime}
}

func NewStatusHelper(cli *clientv3.Client) *StatusHelper {
	return &StatusHelper{Client: cli}
}

func (sh *StatusHelper) WriteStatus(id string, status string, info string) error {
	recordInPrometheus(status)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	statObj := NewStatus(id, status, info, time.Now())
	stat, err := json.Marshal(statObj)
	if err != nil {
		return err
	}
	_, err = sh.Client.Put(ctx, StatusPrefix+id, string(stat))
	return err
}

func (sh *StatusHelper) ReadStatus(id string) (*config.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := sh.Client.Get(ctx, StatusPrefix+id)
	if err != nil {
		return nil, err
	}
	var status config.Status
	for _, v := range resp.Kvs {
		err := json.Unmarshal(v.Value, &status)
		if err != nil {
			return nil, err
		}
	}
	return &status, nil
}

func recordInPrometheus(statusStr string) {
	switch statusStr {
	case config.STATUS_ERROR:
		IncCountOfTaskError()
	case config.STATUS_FINISHED:
		IncCountOfTaskFinished()
	}
}
