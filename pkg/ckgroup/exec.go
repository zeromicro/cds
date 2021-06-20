package ckgroup

import (
	"errors"
	"math/rand"
	"sort"
	"strings"
	"sync"

	"github.com/tal-tech/go-zero/core/logx"
	"golang.org/x/sync/errgroup"
)

type ExecErrDetail struct {
	Err error
	// 发生错误的ckconn对象
	Conn CKConn
}

type AlterErrDetail struct {
	Err error
	// 发生错误的shardconn对象
	Conn       ShardConn
	ShardIndex int
}

func (g *dbGroup) ExecSerialAll(onErrContinue bool, query string, args ...interface{}) ([]ExecErrDetail, error) {
	if isAlterSQL(query) {
		return nil, errors.New("is alert sql")
	}
	var errDetail []ExecErrDetail
	for _, conn := range g.GetAllNodes() {
		err := conn.Exec(query, args...)
		if err != nil {
			errDetail = append(errDetail, ExecErrDetail{Err: err, Conn: conn})
		}
		if !onErrContinue && err != nil {
			return errDetail, nil
		}
	}
	return errDetail, nil
}

func (g *dbGroup) ExecParallelAll(query string, args ...interface{}) ([]ExecErrDetail, error) {
	if isAlterSQL(query) {
		return nil, errors.New("is alert sql")
	}
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(g.GetAllNodes()))
	ch := make(chan ExecErrDetail, len(g.GetAllNodes()))

	for _, conn := range g.GetAllNodes() {
		innerConn := conn
		go func() {
			defer waitGroup.Done()
			if err := innerConn.Exec(query, args...); err != nil {
				ch <- ExecErrDetail{Err: err, Conn: innerConn}
			}
		}()
	}
	waitGroup.Wait()

	close(ch)
	var errs []ExecErrDetail
	for execErrDetail := range ch {
		errs = append(errs, execErrDetail)
	}
	return errs, nil
}

func isAlterSQL(sql string) bool {
	return strings.HasPrefix(strings.TrimSpace(strings.ToLower(sql)), `alter`)
}

func (g *dbGroup) AlterAuto(query string, args ...interface{}) ([]AlterErrDetail, error) {
	if !isAlterSQL(query) {
		return nil, errors.New("not alert sql")
	}

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(g.GetAllShard()))
	ch := make(chan AlterErrDetail, len(g.GetAllShard()))

	for i, conn := range g.GetAllShard() {
		innerConn := conn
		innerShardIndex := i + 1

		go func() {
			defer waitGroup.Done()
			if err := innerConn.AlterAuto(query, args...); err != nil {
				ch <- AlterErrDetail{Err: err, Conn: innerConn, ShardIndex: innerShardIndex}
			}
		}()
	}
	waitGroup.Wait()
	close(ch)
	var errs []AlterErrDetail
	for item := range ch {
		errs = append(errs, item)
	}
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].ShardIndex < errs[j].ShardIndex
	})
	return errs, nil
}

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
	shardConns := g.GetAllShard()[idx].GetAllConn()
	execOrder := rand.Perm(len(shardConns))
	var err error
	for attempt := 1; attempt <= g.opt.RetryNum; attempt++ {
		for _, order := range execOrder {
			err = saveData(shardConns[order].GetRawConn(), query, rows)
			if err == nil {
				return nil
			}
			logx.Errorf("[attempt %d/%d] shard[%d] node[%d] insert error:%s, will switch to next node", attempt, g.opt.RetryNum, idx+1, order+1, err.Error())
			continue
		}
	}
	if err != nil {
		logx.Errorf("shard[%d] all node exec failed. Retry num:%d. Last fail reason: %v, query: %s", idx+1, g.opt.RetryNum, err, query)
	}
	return err
}
