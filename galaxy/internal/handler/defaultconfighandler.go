package handler

import (
	logic2 "github.com/tal-tech/cds/galaxy/internal/logic"
	"net/http"

	"github.com/tal-tech/cds/galaxy/internal/svc"
)

func defaultConfigHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewDefaultConfigLogic(r.Context(), ctx)
		resp := l.DefaultConfig()
		formatFullResponse(resp, nil, w, r)
	}
}
