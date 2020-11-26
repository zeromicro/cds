package logic

import (
	"context"
	"github.com/tal-tech/cds/galaxy/internal/svc"
	"github.com/tal-tech/cds/galaxy/internal/types"

	"github.com/tal-tech/go-zero/core/logx"
)

type ConnectorDeleteLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewConnectorDeleteLogic(ctx context.Context, svcCtx *svc.ServiceContext) ConnectorDeleteLogic {
	return ConnectorDeleteLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *ConnectorDeleteLogic) ConnectorDelete(req types.DeleteConnectorRequest) error {
	var e error
	if req.Type == "connector" {
		if ok, err := l.svcCtx.DebeziumClient.CheckConnectorExists(req.SourceId); ok {
			e = l.svcCtx.DebeziumClient.DeleteConnector(req.SourceId)
		} else {
			e = err
		}
	} else {
		e = l.svcCtx.CanalClient.DeleteInstance(req.SourceId)
	}
	if e != nil {
		logx.Error(e)
		return e
	}
	if err := l.svcCtx.ConnectorModel.DeleteBySourceId(req.SourceId); err != nil {
		logx.Error(err)
		return err
	}
	return nil
}
