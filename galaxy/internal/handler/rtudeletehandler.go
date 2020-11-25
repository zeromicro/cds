package handler

import (
	logic2 "cds/galaxy/internal/logic"
	"cds/galaxy/internal/svc"
	"cds/galaxy/internal/types"
	"github.com/tal-tech/go-zero/rest/httpx"
	"net/http"

	"github.com/tal-tech/go-zero/core/logx"
)

func rtuDeleteHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewRtuDeleteLogic(r.Context(), ctx)
		var req types.String
		if err := httpx.Parse(r, &req); err != nil {
			logx.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := l.RtuDelete(req)
		formatFullResponse(nil, err, w, r)
	}
}
