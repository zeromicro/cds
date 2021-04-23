package ckgroup

import (
	"sort"
	"strings"
	"sync"
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

func (g *dbGroup) ExecSerialAll(onErrContinue bool, query string, args ...interface{}) []ExecErrDetail {
	var errDetail []ExecErrDetail
	for _, conn := range g.GetAllNodes() {
		err := conn.Exec(query, args...)
		if err != nil {
			errDetail = append(errDetail, ExecErrDetail{Err: err, Conn: conn})
		}
		if !onErrContinue && err != nil {
			return errDetail
		}
	}
	return errDetail
}

func (g *dbGroup) ExecParallelAll(query string, args ...interface{}) []ExecErrDetail {
	var errDetail []ExecErrDetail
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(g.GetAllNodes()))

	for _, conn := range g.GetAllNodes() {
		innerConn := conn
		go func() {
			defer waitGroup.Done()
			if err := innerConn.Exec(query, args...); err != nil {
				errDetail = append(errDetail, ExecErrDetail{Err: err, Conn: innerConn})
			}
		}()
	}
	waitGroup.Wait()
	return errDetail
}

func isAlterSQL(sql string) bool {
	return strings.HasPrefix(strings.TrimSpace(strings.ToLower(sql)), `alter`)
}

func (g *dbGroup) AlterAuto(query string, args ...interface{}) []AlterErrDetail {
	isAlterSQL(query)
	var errDetail []AlterErrDetail
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(g.GetAllShard()))

	for i, conn := range g.GetAllShard() {
		innerConn := conn
		innerShardIndex := i + 1

		go func() {
			defer waitGroup.Done()
			if err := innerConn.ExecAuto(query, args...); err != nil {
				errDetail = append(errDetail, AlterErrDetail{Err: err, Conn: innerConn, ShardIndex: innerShardIndex})
			}
		}()
	}
	waitGroup.Wait()
	sort.Slice(errDetail, func(i, j int) bool {
		return errDetail[i].ShardIndex < errDetail[j].ShardIndex
	})
	return errDetail
}
