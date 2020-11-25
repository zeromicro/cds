package handler

import (
	logic2 "cds/galaxy/internal/logic"
	"errors"
	"net/http"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/rest/httpx"

	"cds/galaxy/internal/svc"
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
