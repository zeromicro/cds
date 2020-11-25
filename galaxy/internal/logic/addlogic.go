package logic

import (
	"cds/galaxy/internal/svc"
	"cds/galaxy/internal/types"
	"context"

	"github.com/tal-tech/go-zero/core/logx"
)

type AddLogic struct {
	ctx context.Context
	logx.Logger
}

func NewAddLogic(ctx context.Context, svcCtx *svc.ServiceContext) AddLogic {
	return AddLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
	}
	// TODO need set model here from svc
}

func (l *AddLogic) Add(req types.UserAddRequest) error {
	return nil
}
