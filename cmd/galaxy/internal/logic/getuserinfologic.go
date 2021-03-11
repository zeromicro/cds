package logic

import (
	"context"
	"github.com/tal-tech/cds/cmd/galaxy/internal/model"

	"github.com/tal-tech/cds/cmd/galaxy/internal/svc"
	"github.com/tal-tech/cds/cmd/galaxy/internal/types"

	"github.com/tal-tech/go-zero/core/logx"
)

type GetUserInfoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
	*model.UserModel
}

func NewGetUserInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) GetUserInfoLogic {
	return GetUserInfoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
		UserModel:svcCtx.UserModel,
	}
}

func (l *GetUserInfoLogic) GetUserInfo(req types.GetUserInfoRequest) (*types.GetUserInfoResponse, error) {
	u,e:=l.FindByEmail(req.Email)
	if e!=nil {
		logx.Error(e)
		return nil, e
	}
	return &types.GetUserInfoResponse{
		Roles:        []string{"admin"},
		Name:         u.Name,
		Avatar:       "https://ss3.bdstatic.com/70cFv8Sh_Q1YnxGkpoWK1HF6hhy/it/u=977829211,2120758688&fm=26&gp=0.jpg",
		Introduction: "",
	}, nil
}
