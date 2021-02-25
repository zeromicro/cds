package handler

import (
	"net/http"

	logic2 "github.com/tal-tech/cds/cmd/galaxy/internal/logic"
	"github.com/tal-tech/cds/cmd/galaxy/internal/svc"
	"github.com/tal-tech/cds/cmd/galaxy/internal/types"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/rest/httpx"
)

func rtuStopHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewRtuStopLogic(r.Context(), ctx)
		var req types.String
		if err := httpx.Parse(r, &req); err != nil {
			logx.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := l.RtuStop(req)
		formatFullResponse(nil, err, w, r)
	}
}
