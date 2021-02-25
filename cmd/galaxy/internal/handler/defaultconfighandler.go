package handler

import (
	"net/http"

	logic2 "github.com/tal-tech/cds/cmd/galaxy/internal/logic"
	"github.com/tal-tech/cds/cmd/galaxy/internal/svc"
)

func defaultConfigHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewDefaultConfigLogic(r.Context(), ctx)
		resp := l.DefaultConfig()
		formatFullResponse(resp, nil, w, r)
	}
}
