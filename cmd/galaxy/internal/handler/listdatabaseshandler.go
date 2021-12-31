package handler

import (
	"net/http"

	logic2 "github.com/zeromicro/cds/cmd/galaxy/internal/logic"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
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
		HttpError(w, http.StatusOK, 10001, err.Error(), nil)
		return
	}
	HttpOk(w, resp)
}
