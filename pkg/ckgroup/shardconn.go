package ckgroup

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/tal-tech/go-zero/core/logx"

	"github.com/tal-tech/cds/pkg/ckgroup/config"
)

type ShardConn interface {
	GetAllConn() []CKConn
	GetReplicaConn() []CKConn
	GetShardConn() CKConn

	// Exec 所有节点执行
	Exec(ignoreErr bool, query string, args ...interface{}) []hostErr

	// AlterAuto 随机在一个节点上执行，如果出错自动在下个节点尝试
	AlterAuto(query string, args ...interface{}) error

	// InsertAuto 随机在一个节点上插入，如果出错会自动在下个节点插入
	InsertAuto(query string, sliceData interface{}) error

	Close()
}

type shardConn struct {
	ShardIndex   int
	AllConn      []CKConn
	ShardConn    CKConn
	ReplicaConns []CKConn
}

type hostErr struct {
	Host       string
	Err        error
	NodeIndex  int
	ShardIndex int
}

func NewShardConn(shardIndex int, conf config.ShardGroupConfig) (ShardConn, error) {
	failCount := 0
	shard := shardConn{}
	shard.ShardIndex = shardIndex

	shardNode, err := NewCKConn(conf.ShardNode)
	if err != nil {
		if err == hostParseErr {
			return nil, err
		}
		logx.Errorf("shard[%d] new primary node fail error:%s", shardIndex, err.Error())
		failCount++
	}
	shard.ShardConn = shardNode
	shard.AllConn = append(shard.AllConn, shardNode)
	for _, dns := range conf.ReplicaNodes {
		conn, err := NewCKConn(dns)
		if err != nil {
			if err == hostParseErr {
				return nil, err
			}
			logx.Errorf("shard[%d] new  node fail error:%s", shardIndex, err.Error())
			failCount++
		}
		shard.ReplicaConns = append(shard.ReplicaConns, conn)
		shard.AllConn = append(shard.AllConn, conn)
	}
	if failCount >= len(conf.ReplicaNodes)+1 {
		return nil, errors.New(fmt.Sprintf("shard[%d] all node connection fail", shardIndex))
	}
	return &shard, nil
}

func MustShardConn(shardIndex int, conf config.ShardGroupConfig) ShardConn {
	conn, err := NewShardConn(shardIndex, conf)
	panicIfErr(err)
	return conn
}

func (shardClient *shardConn) GetAllConn() []CKConn {
	return shardClient.AllConn
}

func (shardClient *shardConn) GetReplicaConn() []CKConn {
	return shardClient.ReplicaConns
}

func (shardClient *shardConn) GetShardConn() CKConn {
	return shardClient.ShardConn
}

func (shardClient *shardConn) Exec(ignoreErr bool, query string, args ...interface{}) []hostErr {
	var errs []hostErr
	for i, conn := range shardClient.AllConn {
		if err := conn.Exec(query, args...); err != nil {
			hostErr := hostErr{
				Host:       conn.GetHost(),
				Err:        err,
				NodeIndex:  i + 2,
				ShardIndex: shardClient.ShardIndex,
			}
			errs = append(errs, hostErr)
			if !ignoreErr {
				return errs
			}
		}
	}
	return errs
}

func (shardClient *shardConn) InsertAuto(query string, sliceData interface{}) error {
	conns := shardClient.GetAllConn()
	execOrder := rand.Perm(len(conns))

	var err error
	for _, order := range execOrder {
		err = conns[order].Insert(query, sliceData)
		if err == nil {
			return nil
		}
		logx.Errorf("shard[%d] node[%d] insert error:%s, will switch to next node", shardClient.ShardIndex, order+1, err.Error())
		continue
	}
	if err != nil {
		logx.Errorf("shard[%d] all node insert fail,last error: %s", shardClient.ShardIndex, err.Error())
	}
	return err
}

func (shardClient *shardConn) AlterAuto(query string, args ...interface{}) error {
	conns := shardClient.GetAllConn()
	execOrder := rand.Perm(len(conns))

	var err error
	for _, order := range execOrder {
		err = conns[order].Exec(query, args...)
		if err == nil {
			return nil
		}
		logx.Errorf("shard[%d] node[%d] exec error:%s, will switch to next node", shardClient.ShardIndex, order+1, err.Error())
		continue
	}
	if err != nil {
		logx.Errorf("shard[%d] all node exec fail,last error: %s", shardClient.ShardIndex, err.Error())
	}
	return err
}

func (shardClient *shardConn) Close() {
	for _, conn := range shardClient.AllConn {
		if err := conn.GetRawConn().Close(); err != nil {
			logx.Error(err)
		}
	}
}
