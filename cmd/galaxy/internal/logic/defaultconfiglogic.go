package logic

import (
	"context"
	"strings"

	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/cmd/galaxy/internal/types"
	"github.com/zeromicro/cds/pkg/strx"
	"github.com/zeromicro/go-zero/core/logx"
)

type DefaultConfigLogic struct {
	ctx context.Context
	logx.Logger
	svcCtx *svc.ServiceContext
}

func NewDefaultConfigLogic(ctx context.Context, svcCtx *svc.ServiceContext) DefaultConfigLogic {
	return DefaultConfigLogic{
		ctx:    ctx,
		Logger: logx.WithContext(ctx),
		svcCtx: svcCtx,
	}
}

func (d *DefaultConfigLogic) DefaultConfig() *types.DefaultConfigResponse {
	queryNode := d.svcCtx.Config.CkDataNodes[0][:strings.Index(d.svcCtx.Config.CkDataNodes[0], "?")]
	shard := strx.DeepSplit(d.svcCtx.Config.CkDataNodes[1:], ",")
	for i := 0; i < len(shard); i++ {
		for j := 0; j < len(shard[i]); j++ {
			shard[i][j] = shard[i][j][:strings.Index(shard[i][j], "?")]
		}
	}
	return &types.DefaultConfigResponse{
		QueryNode: queryNode,
		Shards:    shard,
		Cluster:   "bip_ck_cluster",
	}
}
