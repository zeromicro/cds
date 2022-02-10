package logic

import (
	"context"

	"github.com/zeromicro/cds/cmd/galaxy/internal/model"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewGetLogic(ctx context.Context, svcCtx *svc.ServiceContext) GetLogic {
	return GetLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *GetLogic) Get(user *model.User) (*types.UserGetResponse, error) {
	return &types.UserGetResponse{
		Email:   user.Email,
		GroupId: user.GroupID,
		Name:    user.Name,
	}, nil
}
