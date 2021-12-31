package logic

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/zeromicro/cds/cmd/galaxy/internal/clients"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
	"github.com/zeromicro/cds/cmd/rtu/cmd/sync/config"
	"github.com/zeromicro/cds/pkg/strx"
)

type RtuRedoLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewRtuRedoLogic(ctx context.Context, svcCtx *svc.ServiceContext) RtuRedoLogic {
	return RtuRedoLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *RtuRedoLogic) RtuRedo(req types.String) error {
	id, e := strconv.Atoi(req.String)
	if e != nil {
		logx.Error(e)
		return e
	}
	e = l.svcCtx.RtuModel.Update(id, "doing")
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
		job, err := buildJob(dsn, strconv.Itoa(i.ID), i.SourceType, i.SourceTable, i.SourceQueryKey, i.TargetDB, *shards)
		if err != nil {
			logx.Error(err)
			continue
		}
		jobs = append(jobs, *job)
	}
	cli := clients.NewRtuClient(l.svcCtx.EtcdClient)
	_, err = cli.Delete(req.String)
	if err != nil {
		logx.Error(e)
		return e
	}
	e = cli.StartJobs(jobs)
	if e != nil {
		logx.Error(e)
		return e
	}
	logx.Info(jobs)
	return nil
}
