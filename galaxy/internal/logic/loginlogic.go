package logic

import (
	"context"
	"errors"

	"github.com/tal-tech/cds/galaxy/internal/svc"
	"github.com/tal-tech/cds/galaxy/internal/types"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

type LoginLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) LoginLogic {
	return LoginLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *LoginLogic) Login(req types.UserLoginRequest) (*types.TokenResponse, error) {
	u, e := l.svcCtx.UserModel.FindByEmail(req.Email)
	if e != nil {
		if e == sqlx.ErrNotFound {
			return nil, errors.New("账户未找到")
		}
		logx.Error(e)
		return nil, e
	}
	if u.Password != req.Password {
		return nil, errors.New("密码错误")
	}
	token, e := l.svcCtx.UserModel.UpdateToken(u.ID)
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	return &types.TokenResponse{Token: token}, nil
}
