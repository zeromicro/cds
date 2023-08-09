package logic

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/cds/cmd/dm/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/galaxy/internal/model"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
	"github.com/zeromicro/cds/pkg/mongodbx"
	"github.com/zeromicro/cds/pkg/strx"
	"github.com/zeromicro/go-zero/core/logx"
)

type RtuAddLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewRtuAddLogic(ctx context.Context, svcCtx *svc.ServiceContext) RtuAddLogic {
	return RtuAddLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *RtuAddLogic) RtuAdd(req types.RtuModel) (*types.RtuModel, error) {
	sourceType := "canal-" + config.TYPE_MYSQL
	if strings.HasPrefix(req.Source.Dsn, "mongodb://") {
		req.Source.QueryKey = []string{"_id"}
		sourceType = "connector-" + config.TYPE_MONGODB
	}
	// 修复当选择多个表时出错的情况，用SelectedTable数组个数赋值给QueryKey
	if len(req.Source.QueryKey) == 0 {
		req.Source.QueryKey = req.Source.SelectedTable
	}
	//else {
	//	req.Source.QueryKey = []string{""}
	//}
	shards, e := json.Marshal(l.svcCtx.Config.CkDataNodes[1:])
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	var sourceDb string
	if strings.Contains(strings.ToLower(sourceType), "mysql") {
		tmp, err := mysql.ParseDSN(req.Source.Dsn)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		sourceDb = tmp.DBName
	} else {
		tmp, err := mongodbx.ParseDsn(req.Source.Dsn)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		sourceDb = tmp.Database
	}
	for k, v := range req.Source.SelectedTable {
		_, e := l.svcCtx.RtuModel.Insert(&model.Rtu{
			Name:           req.Target.SelectedDatabase + "." + v,
			SourceType:     sourceType,
			SourceDsn:      strx.EncryptDsn(req.Source.Dsn),
			SourceTable:    v,
			SourceDb:       sourceDb,
			SourceQueryKey: req.Source.QueryKey[k],
			TargetType:     config.TYPE_CLICKHOUSE,
			TargetShards:   strx.EncryptDsn(string(shards)),
			TargetDB:       req.Target.SelectedDatabase,
			TargetChProxy:  "",
			TargetTable:    v,
		})
		if e != nil {
			if err, ok := e.(*mysql.MySQLError); ok {
				if err.Number == 1062 {
					return nil, errors.New("任务已存在")
				}
			}
			logx.Error(e)
			return nil, e
		}
	}

	return &req, nil
}
