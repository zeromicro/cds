package logic

import (
	"context"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/tal-tech/cds/cmd/galaxy/internal/model"
	"github.com/tal-tech/cds/cmd/galaxy/internal/svc"
	"github.com/tal-tech/cds/cmd/galaxy/internal/types"
	"github.com/tal-tech/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

type ConnectorAddLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewConnectorAddLogic(ctx context.Context, svcCtx *svc.ServiceContext) ConnectorAddLogic {
	return ConnectorAddLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *ConnectorAddLogic) ConnectorAdd(req types.ConnectorModel) error {
	for _, table := range req.Source.SelectedTable {
		var db, sourceType, sourceId string
		if strings.HasPrefix(req.Source.Dsn, "mongodb://") {
			info, err := connstring.Parse(req.Source.Dsn)
			if err != nil {
				logx.Error(err)
				return err
			}
			db = info.Database
			_, err = l.svcCtx.DebeziumClient.ListenMongo(req.Source.Dsn, db, table, req.Source.Suffix)
			if err != nil {
				logx.Error(err)
				return err
			}
			sourceType = "connector"
			sourceId = "mongoconnector." + db + "." + table
		} else {
			info, err := mysql.ParseDSN(req.Source.Dsn)
			if err != nil {
				logx.Error(err)
				return err
			}
			db = info.DBName
			canalConfig := l.svcCtx.Config.CanalConfig
			if err := l.svcCtx.CanalClient.AddInstances(info, table, canalConfig.ServerID); err != nil {
				logx.Error(err)
				return err
			}
			sourceType = "canal"
			instance, err := l.svcCtx.CanalClient.GetInstanceByName("canal_" + db + "_" + table)
			if err != nil {
				logx.Error(err)
				return err
			}
			sourceId = strconv.Itoa(instance.Id)
		}
		connector := model.Connector{SourceDB: db, SourceTable: table, SourceType: sourceType, SourceId: sourceId}
		if _, err := l.svcCtx.ConnectorModel.Insert(&connector); err != nil {
			return err
		}
	}
	return nil
}
