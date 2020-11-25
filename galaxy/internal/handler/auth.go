package handler

import (
	"errors"
	"net/http"

	"github.com/tal-tech/go-zero/core/stores/sqlx"
	"github.com/tal-tech/go-zero/rest/httpx"

	"github.com/tal-tech/cds/galaxy/internal/model"
	"github.com/tal-tech/cds/galaxy/internal/svc"
)

func handleToken(ctx *svc.ServiceContext, w http.ResponseWriter, r *http.Request) (*model.User, error) {
	token, e := r.Cookie("token")
	if e != nil {
		httpx.Error(nil, errors.New("Token已过期，请重新登陆"))
		return nil, e
	}
	user, e := ctx.UserModel.FindByToken(token.Value)
	if e != nil {
		if e == sqlx.ErrNotFound {
			httpx.Error(nil, errors.New("Token已过期，请重新登陆"))
			return nil, e
		}
		return nil, e
	}
	return user, nil
}
