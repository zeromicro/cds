package logic

import (
	"context"
	"unsafe"

	"github.com/tal-tech/go-zero/core/logx"

	"github.com/tal-tech/cds/galaxy/internal/svc"
	"github.com/tal-tech/cds/galaxy/internal/types"
)

type ConnectorListLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewConnectorListLogic(ctx context.Context, svcCtx *svc.ServiceContext) ConnectorListLogic {
	return ConnectorListLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *ConnectorListLogic) ConnectorList(req types.ListRequest) (*types.ConnectorListResponse, error) {
	cnt, err := l.svcCtx.ConnectorModel.GetCountByDb(req.DbName)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	data, err := l.svcCtx.ConnectorModel.FindByDb(req.DbName, (req.PageAndSize.Page-1)*req.PageAndSize.Size, req.PageAndSize.Size)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	connectors := make([]*types.Connector, 0, len(data))
	for _, v := range data {
		connector := types.Connector{
			CreateTime:  v.CreateTime.Format("2006-01-02 15:04:05"),
			UpdateTime:  v.UpdateTime.Format("2006-01-02 15:04:05"),
			SourceType:  v.SourceType,
			SourceId:    v.SourceId,
			SourceDB:    v.SourceDB,
			SourceTable: v.SourceTable,
		}
		connectors = append(connectors, &connector)
	}
	var connectListResp types.ConnectorListResponse
	connectListResp.ConnectorList = connectors
	connectListResp.PageAndSize = types.PageAndSize{Page: req.Page, Size: *(*int)(unsafe.Pointer(&cnt))}
	return &connectListResp, nil
}
