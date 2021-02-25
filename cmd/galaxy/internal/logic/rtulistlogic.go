package logic

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/tal-tech/cds/cmd/galaxy/internal/clients"
	"github.com/tal-tech/cds/cmd/galaxy/internal/svc"
	"github.com/tal-tech/cds/cmd/galaxy/internal/types"
	"github.com/tal-tech/cds/cmd/rtu/cmd/sync/config"
	"github.com/tal-tech/cds/pkg/strx"
	"github.com/tal-tech/cds/pkg/timex"
	"github.com/tal-tech/go-zero/core/logx"
)

type RtuListLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewRtuListLogic(ctx context.Context, svcCtx *svc.ServiceContext) RtuListLogic {
	return RtuListLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *RtuListLogic) RtuList(req types.ListRequest) (*types.RtuList, error) {
	cli := clients.NewRtuClient(l.svcCtx.EtcdClient)
	sts, e := cli.All()
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	stMap := make(map[int]*config.Status)
	for _, st := range sts {
		id, e := strconv.Atoi(st.ID)
		if e != nil {
			continue
		}
		stMap[id] = st
	}

	vs, e := l.svcCtx.RtuModel.FindByDb(req.DbName, req.Page, req.Size)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	rp := &types.RtuList{}

	cnt, err := l.svcCtx.RtuModel.GetCountByDb(req.DbName)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	for _, v := range vs {
		rtu := types.Rtu{
			ID:             v.ID,
			Name:           v.Name,
			SourceType:     v.SourceType,
			SourceDsn:      v.SourceDsn,
			SourceTable:    v.SourceTable,
			SourceQueryKey: v.SourceQueryKey,
			SourceTopic:    v.SourceTopic,
			TargetType:     v.TargetType,
			TargetDB:       v.TargetDB,
			TargetChProxy:  v.TargetChProxy,
			TargetTable:    v.TargetTable,
			CreateTime:     v.CreateTime.Format(timex.StandardLayout),
		}
		if v.TargetShards != "" {
			vs := []string{}
			shards, e := strx.DecryptDsn(v.TargetShards)
			if e != nil {
				logx.Error(e)
				return nil, e
			}
			e = json.Unmarshal([]byte(shards), &vs)
			if e != nil {
				logx.Error(e)
				return nil, e
			}
			rtu.TargetShards = strx.DeepSplit(vs, ",")
		}

		if job, ok := stMap[rtu.ID]; ok {
			rtu.Status = job.Status
			rtu.Information = job.Information
			rtu.UpdateTime = job.UpdateTime.Format(timex.StandardLayout)
			if time.Now().Unix()-job.UpdateTime.Unix() > 60 && rtu.Status == "running" {
				rtu.Status = "stopped"
				rtu.Information = "任务超时"
				rtu.UpdateTime = job.UpdateTime.Format(timex.StandardLayout)
			}

		} else {
			rtu.Status = "未启动"
		}

		rp.RtuList = append(rp.RtuList, rtu)
	}
	rp.PageAndSize = types.PageAndSize{
		Size: int(cnt),
		Page: req.Page,
	}
	return rp, nil
}
