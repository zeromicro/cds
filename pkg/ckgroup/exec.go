package ckgroup

import (
	"errors"

	"github.com/tal-tech/go-zero/core/logx"
	"golang.org/x/sync/errgroup"
)

func (g *dbGroup) ExecAuto(query string, hashIdx int, args [][]interface{}) error {
	if len(args) == 0 || len(args[0]) == 0 || hashIdx >= len(args[0]) {
		return errors.New("can not get hashIdx value")
	}

	dataBatch, err := getDataBatch(hashIdx, len(g.ShardNodes), args)
	if err != nil {
		return err
	}

	var eg errgroup.Group
	for idx, rows := range dataBatch {
		if len(rows) == 0 {
			continue
		}
		idxInternal := idx
		rowsInternal := rows
		eg.Go(func() error {
			return g.exec(idxInternal, query, rowsInternal)
		})
	}
	err = eg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (g *dbGroup) ExecAll(query string, args [][]interface{}) error {
	rows := make([]rowValue, 0, len(args))
	if len(args) == 0 {
		rows = append(rows, rowValue{})
	} else {
		rows = append(rows, args...)
	}

	var eg errgroup.Group
	for i := 0; i < len(g.ShardNodes); i++ {
		index := i
		eg.Go(func() error {
			return g.exec(index, query, rows)
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (g *dbGroup) exec(idx int, query string, rows []rowValue) error {
	var err error
	for attempt := 1; attempt <= g.opt.RetryNum; attempt++ {
		err = execOnNode(g.ShardNodes[idx].GetShardConn().GetRawConn(), query, rows)
		if err != nil {
			logx.Infof("[attempt %d/%d] Node[%d] primary node execute error:%v, will switch to replica node", attempt, g.opt.RetryNum, idx, err)
		} else {
			return nil
		}
		for i, replicaNode := range g.ShardNodes[idx].GetReplicaConn() {
			err = execOnNode(replicaNode.GetRawConn(), query, rows)
			if err != nil {
				logx.Infof("[attempt %d/%d] Node[%d] replica[%d] execute error:%v, will switch to next replica node", attempt, g.opt.RetryNum, idx, i, err)
			} else {
				return nil
			}
		}
	}
	if err != nil {
		logx.Errorf("All node exec failed. Retry num:%d. Last fail reason: %v, query: %s", g.opt.RetryNum, err, query)
	}
	return err
}
