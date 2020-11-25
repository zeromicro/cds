package handler

import (
	logic2 "cds/galaxy/internal/logic"
	"cds/galaxy/internal/svc"
	"cds/galaxy/internal/types"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/rest/httpx"
	"net/http"
)

func rtuListHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewRtuListLogic(r.Context(), ctx)
		var req types.ListRequest
		if err := httpx.Parse(r, &req); err != nil {
			logx.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := l.RtuList(req)
		formatFullResponse(resp, err, w, r)
	}
}
