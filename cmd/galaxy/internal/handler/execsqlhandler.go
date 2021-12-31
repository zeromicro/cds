package handler

import (
	"net/http"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/rest/httpx"
	logic2 "github.com/zeromicro/cds/cmd/galaxy/internal/logic"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
)

func execSqlHandler(ctx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		l := logic2.NewExecSqlLogic(r.Context(), ctx)
		var req types.ExecSqlRequest
		if err = httpx.Parse(r, &req); err != nil {
			logx.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var execResp types.ExecSqlResponse
		for _, v := range req.Sql {
			err = l.ExecSql(ctx.Config.CkDataNodes[0], v)
			if err != nil {
				execResp.FailedTables = append(execResp.FailedTables, v)
				execResp.FailedReasons = append(execResp.FailedReasons, err.Error())
				formatFullResponse(execResp, err, w, r)
				return
			}
		}
		formatFullResponse(execResp, nil, w, r)
	}
}
