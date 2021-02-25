package choperator

import (
	"github.com/tal-tech/go-zero/core/logx"

	"github.com/tal-tech/cds/cmd/dm/util"
	"github.com/tal-tech/cds/pkg/ckgroup"
	ckcfg "github.com/tal-tech/cds/pkg/ckgroup/config"
)

type ChOperator interface {
	MysqlBatchInsert(insertData [][]interface{}, insertQuery string, arr []util.DataType, indexOfFlag, indexOfInsertID int, indexOfPrimKeys int) error
	ObtainClickHouseKV(targetDB, targetTable string) (map[string]string, error)
	BatchInsert(insertData [][]interface{}, insertQuery string, indexOfPrimKey int) error
}

func NewChOperator(shards [][]string) (ChOperator, error) {
	shardCfgs := make([]ckcfg.ShardGroupConfig, 0, len(shards))
	for _, i := range shards {
		cfg := ckcfg.ShardGroupConfig{ReplicaNodes: make([]string, 0, len(i)-1)}
		for index, addr := range i {
			if index == 0 {
				cfg.ShardNode = addr
			} else {
				cfg.ReplicaNodes = append(cfg.ReplicaNodes, addr)
			}
		}
		shardCfgs = append(shardCfgs, cfg)
	}
	ckConfig := ckcfg.Config{
		ShardGroups: shardCfgs,
		QueryNode:   shardCfgs[0].ShardNode,
	}
	ch, err := ckgroup.NewCKGroup(ckConfig)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	var cgo CkGroupOperator
	cgo.ckGroup = ch
	return &cgo, nil
}
