package logic

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/cds/cmd/dm/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/galaxy/internal/clients"
	"github.com/zeromicro/cds/cmd/galaxy/internal/model"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
	"github.com/zeromicro/cds/pkg/mongodbx"
	"github.com/zeromicro/cds/pkg/strx"
	"github.com/zeromicro/go-zero/core/logx"
)

type DmAddLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewDmAddLogic(ctx context.Context, svcCtx *svc.ServiceContext) DmAddLogic {
	return DmAddLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *DmAddLogic) DmAdd(req types.DmModel) (*types.DmModel, error) {
	sourceType := config.TYPE_MYSQL
	if strings.HasPrefix(req.Source.Dsn, "mongodb://") {
		sourceType = config.TYPE_MONGODB
	}
	shards, e := json.Marshal(l.svcCtx.Config.CkDataNodes[1:])
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	start, e := strconv.Atoi(req.WindowStartHour)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	end, e := strconv.Atoi(req.WindowEndHour)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	if start < 0 || start > 23 || end > 23 || end < 0 {
		return nil, errors.New("窗口期范围只能时0~23")
	}
	cli := clients.NewDmClient(l.svcCtx.EtcdClient)
	jobs, err := getHistoryJobs(l.svcCtx.DmModel)
	if err != nil {
		logx.Error(err)
		return nil, err
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
	ids := make([]int, 0, len(req.Source.SelectedTable))
	for k, v := range req.Source.SelectedTable {
		id, e := l.svcCtx.DmModel.Insert(&model.Dm{
			Name:            req.Target.SelectedDatabase + "." + v,
			SourceType:      sourceType,
			SourceDsn:       strx.EncryptDsn(req.Source.Dsn),
			SourceTable:     v,
			SourceQueryKey:  req.Source.QueryKey[k],
			TargetType:      config.TYPE_CLICKHOUSE,
			TargetShards:    strx.EncryptDsn(string(shards)),
			TargetDB:        req.Target.SelectedDatabase,
			TargetChProxy:   "",
			TargetTable:     v,
			WindowStartHour: start,
			WindowEndHour:   end,
			Suffix:          req.Source.Suffix,
		})
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		job := config.Job{
			ID: strconv.Itoa(id),
		}
		job.Source.Type = sourceType
		job.Source.Table = v
		job.Source.Dsn = req.Source.Dsn
		job.Source.DB = sourceDb
		job.Source.QueryKey = req.Source.QueryKey[k]
		job.Source.Suffix = req.Source.Suffix

		job.Target.Type = config.TYPE_CLICKHOUSE
		job.Target.Shards = strx.DeepSplit(l.svcCtx.Config.CkDataNodes[1:], ",")
		job.Target.ChProxy = ""
		job.Target.DB = req.Target.SelectedDatabase
		job.Target.Table = v
		jobs = append(jobs, job)
		ids = append(ids, id)
	}
	e = cli.StartJobs(jobs)
	if e != nil {
		logx.Error(e)
		for _, v := range ids {
			err := l.svcCtx.DmModel.Delete(v)
			if err != nil {
				logx.Error(err)
			}
		}
		return nil, e
	}

	return &req, nil
}
