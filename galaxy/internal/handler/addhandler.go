package handler

import (
	logic2 "cds/galaxy/internal/logic"
	"net/http"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/rest/httpx"

	"cds/galaxy/internal/svc"
	"cds/galaxy/internal/types"
)

func addHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewAddLogic(r.Context(), ctx)
		var req types.UserAddRequest
		if err := httpx.Parse(r, &req); err != nil {
			logx.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := l.Add(req)
		if err != nil {
			httpx.Error(w, err)
		}
		httpx.Ok(w)
	}
}
