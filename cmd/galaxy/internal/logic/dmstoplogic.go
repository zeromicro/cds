package logic

import (
	"context"
	"errors"

	"github.com/zeromicro/cds/cmd/dm/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/galaxy/internal/clients"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
	"github.com/zeromicro/go-zero/core/logx"
)

type DmStopLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewDmStopLogic(ctx context.Context, svcCtx *svc.ServiceContext) DmStopLogic {
	return DmStopLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *DmStopLogic) DmStop(req types.String) error {
	cli := clients.NewDmClient(l.svcCtx.EtcdClient)
	status, e := cli.Status(req.String)
	if e != nil {
		logx.Error(e)
		return e
	}
	if status.Status != config.STATUS_RUNNING {
		return errors.New("状态为" + status.Status + "，无需停止")
	}
	e = cli.StopJob(req.String)
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}
