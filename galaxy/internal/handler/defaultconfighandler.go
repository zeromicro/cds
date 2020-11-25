package handler

import (
	logic2 "cds/galaxy/internal/logic"
	"net/http"

	"cds/galaxy/internal/svc"
)

func defaultConfigHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewDefaultConfigLogic(r.Context(), ctx)
		resp := l.DefaultConfig()
		formatFullResponse(resp, nil, w, r)
	}
}
