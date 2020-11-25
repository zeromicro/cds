package logic

import (
	"cds/galaxy/internal/clients"
	"cds/galaxy/internal/svc"
	"cds/galaxy/internal/types"
	"cds/rtu/cmd/sync/config"
	"cds/tools/strx"
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/tal-tech/go-zero/core/logx"
)

type RtuStopLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewRtuStopLogic(ctx context.Context, svcCtx *svc.ServiceContext) RtuStopLogic {
	return RtuStopLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *RtuStopLogic) RtuStop(req types.String) error {
	cli := clients.NewRtuClient(l.svcCtx.EtcdClient)
	status, e := cli.Status(req.String)
	if e != nil {
		logx.Error(e)
		return e
	}
	id, err := strconv.Atoi(req.String)
	if err != nil {
		logx.Error(e)
		return e
	}
	e = l.svcCtx.RtuModel.Update(id, "stop")
	if e != nil {
		logx.Error(e)
		return e
	}
	exists, err := l.svcCtx.RtuModel.GetExist()
	if err != nil {
		logx.Error(e)
		return e
	}
	jobs := make([]config.Job, 0, len(exists))
	for _, i := range exists {
		s, err := strx.DecryptDsn(i.TargetShards)
		if err != nil {
			logx.Error(err)
			continue
		}
		shards := new([]string)
		err = json.Unmarshal([]byte(s), shards)
		if err != nil {
			logx.Error(err)
			continue
		}
		dsn, err := strx.DecryptDsn(i.SourceDsn)
		if err != nil {
			logx.Error(err)
			continue
		}
		job, err := buildJob(dsn, strconv.Itoa(i.ID), i.SourceType, i.TargetTable, i.SourceQueryKey, i.TargetDB, *shards)
		if err != nil {
			logx.Error(err)
			continue
		}
		jobs = append(jobs, *job)
	}
	err = cli.StartJobs(jobs)
	if err != nil {
		logx.Error(err)
		return err
	}

	if status.Status != config.STATUS_RUNNING {
		return errors.New("状态为" + status.Status + ",无需停止")
	}

	return nil
}
