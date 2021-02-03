package logic

import (
	"context"
	"errors"
	"github.com/dgrijalva/jwt-go"
	"time"

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
	var info = map[string]interface{}{
		"name":  u.Name,
		"email": u.Email,
		"id":    u.ID,
		"perm":  u.GroupID,
	}
	auth, err := genToken(l.svcCtx.Config.Auth.AccessSecret, info, 24*3600)
	if err != nil {
		logx.Error(err)
		return nil, err
	}

	return &types.TokenResponse{Auth: auth}, nil
}

func genToken(secretKey string, payloads map[string]interface{}, seconds int64) (string, error) {
	now := time.Now().Unix()
	claims := make(jwt.MapClaims)
	claims["exp"] = now + seconds
	claims["iat"] = now
	for k, v := range payloads {
		claims[k] = v
	}

	token := jwt.New(jwt.SigningMethodHS256)
	token.Claims = claims

	return token.SignedString([]byte(secretKey))
}
