package logic

import (
	"context"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/zeromicro/cds/cmd/galaxy/internal/clients"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
)

type DmRedoLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewDmRedoLogic(ctx context.Context, svcCtx *svc.ServiceContext) DmRedoLogic {
	return DmRedoLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *DmRedoLogic) DmRedo(req types.String) error {
	_, e := clients.NewDmClient(l.svcCtx.EtcdClient).Delete(req.String)
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}
