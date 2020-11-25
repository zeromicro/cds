package handler

import (
	logic2 "cds/galaxy/internal/logic"
	"cds/galaxy/internal/svc"
	"github.com/tal-tech/go-zero/rest/httpx"
	"net/http"
)

func listDatabasesHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewListDatabasesLogic(r.Context(), ctx)
		resp, err := l.ListDatabases(ctx.Config.CkDataNodes[0])
		formatFullResponse(resp, err, w, r)
	}
}

func formatFullResponse(resp interface{}, err error, w http.ResponseWriter, r *http.Request) {
	if err != nil {
		httpx.Error(w, err)
		return
	}
	httpx.WriteJson(w, http.StatusOK, resp)
}
