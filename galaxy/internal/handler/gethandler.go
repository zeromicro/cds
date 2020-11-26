package handler

import (
	"errors"
	logic2 "github.com/tal-tech/cds/galaxy/internal/logic"
	"net/http"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/rest/httpx"

	"github.com/tal-tech/cds/galaxy/internal/svc"
)

func getHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewGetLogic(r.Context(), ctx)
		user, e := handleToken(ctx, w, r)
		if e != nil {
			logx.Error(e)
			return
		}
		resp, err := l.Get(user)
		if err != nil {
			httpx.Error(nil, errors.New("Token已过期，请重新登陆"))
		}
		httpx.WriteJson(w, http.StatusOK, resp)
	}
}
