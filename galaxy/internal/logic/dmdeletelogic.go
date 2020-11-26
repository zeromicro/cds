package logic

import (
	"context"
	"strconv"

	"github.com/tal-tech/cds/galaxy/internal/clients"
	"github.com/tal-tech/cds/galaxy/internal/svc"
	"github.com/tal-tech/cds/galaxy/internal/types"
	"github.com/tal-tech/go-zero/core/logx"
)

type DmDeleteLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewDmDeleteLogic(ctx context.Context, svcCtx *svc.ServiceContext) DmDeleteLogic {
	return DmDeleteLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *DmDeleteLogic) DmDelete(req types.String) error {
	i, e := strconv.Atoi(req.String)
	if e != nil {
		logx.Error(e)
		return e
	}
	e = l.svcCtx.DmModel.Delete(i)
	if e != nil {
		logx.Error(e)
		return e
	}
	dms, err := getHistoryJobs(l.svcCtx.DmModel)
	if err != nil {
		logx.Error(err)
		return err
	}
	cli := clients.NewDmClient(l.svcCtx.EtcdClient)
	e = cli.StartJobs(dms)
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}
