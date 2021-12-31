package logic

import (
	"context"
	"strconv"

	"github.com/tal-tech/go-zero/core/logx"
	dmConfig "github.com/zeromicro/cds/cmd/dm/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/galaxy/internal/clients"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
)

type DatabaseListLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewDatabaseListLogic(ctx context.Context, svcCtx *svc.ServiceContext) DatabaseListLogic {
	return DatabaseListLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
	// TODO need set model here from svc
}

func (l *DatabaseListLogic) DatabaseList(req types.String) (*types.StringList, error) {
	var resp types.StringList
	var databaseList []string
	var err error
	var mp map[string]int
	switch req.String {
	case "dm":
		mp, err = l.getDmList()
		for k := range mp {
			databaseList = append(databaseList, k)
		}
	case "rtu":
		mp, err = l.getRtuList()
		for k := range mp {
			databaseList = append(databaseList, k)
		}
	default:
		databaseList, err = l.svcCtx.ConnectorModel.GetAllDb()
	}
	if err != nil {
		return nil, err
	}
	resp.StringList = databaseList
	return &resp, nil
}

func (l *DatabaseListLogic) getDmList() (map[string]int, error) {
	mp := make(map[string]int)
	cli := clients.NewDmClient(l.svcCtx.EtcdClient)
	sts, e := cli.All()
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	stMap := make(map[int]*dmConfig.Status)
	ids := []int{}
	for _, st := range sts {
		id, e := strconv.Atoi(st.ID)
		if e != nil {
			continue
		}
		stMap[id] = st
		ids = append(ids, id)
	}
	vs, e := l.svcCtx.DmModel.FindIn(ids...)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	for _, v := range vs {
		mp[v.TargetDB]++
	}
	return mp, nil
}

func (l *DatabaseListLogic) getRtuList() (map[string]int, error) {
	dbs, err := l.svcCtx.RtuModel.GetAllDb()
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	mp := make(map[string]int, 10)
	for _, db := range dbs {
		mp[db.SourceDb]++
	}
	return mp, nil
}
