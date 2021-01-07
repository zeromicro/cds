package handler

import (
	"net/http"

	logic2 "github.com/tal-tech/cds/galaxy/internal/logic"
	"github.com/tal-tech/cds/galaxy/internal/svc"
	"github.com/tal-tech/go-zero/rest/httpx"
)

func ListDatabasesHandler(ctx *svc.ServiceContext) http.HandlerFunc {
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
