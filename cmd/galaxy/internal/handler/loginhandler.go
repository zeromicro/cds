package handler

import (
	"net/http"

	logic2 "github.com/zeromicro/cds/cmd/galaxy/internal/logic"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func loginHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic2.NewLoginLogic(r.Context(), ctx)
		var req types.UserLoginRequest
		if err := httpx.Parse(r, &req); err != nil {
			logx.Error(err)
			httpx.Error(w, err)
			return
		}

		resp, err := l.Login(req)
		formatFullResponse(resp, err, w, r)
	}
}
