package logic

import (
	"cds/galaxy/internal/svc"
	"cds/galaxy/internal/types"
	"cds/tools/mongodbx"
	"cds/tools/mysqlx"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/tal-tech/go-zero/core/logx"
)

type ListTableLogic struct {
	ctx context.Context
	logx.Logger
}

func NewListTableLogic(ctx context.Context, svcCtx *svc.ServiceContext) ListTableLogic {
	return ListTableLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
	}
	// TODO need set model here from svc
}

func (l *ListTableLogic) ListTable(req types.String) (*types.StringList, error) {
	if strings.HasPrefix(req.String, "mongodb") {
		cli, e := mongodbx.TakeMongoClient(req.String)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		out, e := mongodbx.ListCollections(cli, req.String)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		sort.Strings(out)
		return &types.StringList{StringList: out}, nil
	}
	cli, e := mysqlx.TakeMySQLConn(req.String)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	tables, e := mysqlx.MysqlListTable(cli)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	fmt.Println(tables)

	sort.Strings(tables)
	return &types.StringList{StringList: tables}, nil
}
