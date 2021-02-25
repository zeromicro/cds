package logic

import (
	"context"
	"strings"

	"github.com/tal-tech/cds/galaxy/internal/svc"
	"github.com/tal-tech/cds/pkg/clickhousex"
	"github.com/tal-tech/go-zero/core/logx"
)

type ExecSqlLogic struct {
	ctx context.Context
	logx.Logger
}

func NewExecSqlLogic(ctx context.Context, svcCtx *svc.ServiceContext) ExecSqlLogic {
	return ExecSqlLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
	}
	// TODO need set model here from svc
}

func (l *ExecSqlLogic) ExecSql(dsn, sql string) error {
	cli := clickhousex.TakeClientx(dsn)
	for _, sql := range strings.Split(sql, ";") {
		if sql == "" {
			continue
		}
		_, e := cli.Exec(sql)
		if e != nil {
			logx.Error(e)
			return e
		}
	}
	return nil
}
