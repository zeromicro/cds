package ckgroup

import (
	"github.com/tal-tech/go-zero/core/logx"

	"github.com/tal-tech/cds/pkg/ckgroup/config"
)

type ShardConn interface {
	GetAllConn() []CKConn
	GetReplicaConn() []CKConn
	GetShardConn() CKConn
	//所有节点执行
	Exec(ignoreErr bool, query string, args ...interface{}) []hostErr
	//所有副本节点执行
	ExecReplica(ignoreErr bool, query string, args ...interface{}) []hostErr
	//在主节点上执行,如果失败在副本节点上执行
	ExecAuto(query string, args ...interface{}) error
	//在主节点上插入,如果失败在副本节点上插入
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
	//失败时,close 所有数据库连接
	isClean := false
	shard := shardConn{}
	shard.ShardIndex = shardIndex

	defer func() {
		if isClean {
			shard.Close()
		}
	}()

	shardNode, err := NewCKConn(conf.ShardNode)
	if err != nil {
		return nil, err
	}
	shard.ShardConn = shardNode
	shard.AllConn = append(shard.AllConn, shardNode)
	for _, dns := range conf.ReplicaNodes {
		conn, err := NewCKConn(dns)
		if err != nil {
			isClean = true
			return nil, err
		}
		shard.ReplicaConns = append(shard.ReplicaConns, conn)
		shard.AllConn = append(shard.AllConn, conn)
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

func (shardClient *shardConn) ExecReplica(ignoreErr bool, query string, args ...interface{}) []hostErr {
	var errs []hostErr
	for i, conn := range shardClient.ReplicaConns {
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
	err := shardClient.GetShardConn().Insert(query, sliceData)
	if err == nil {
		return nil
	}
	logx.Infof("shard[%d] primary node insert error:%v, will switch to replica node", shardClient.ShardIndex, err)
	for i, replicaConn := range shardClient.GetReplicaConn() {
		index := i + 1
		err = replicaConn.Insert(query, sliceData)
		if err == nil {
			return nil
		}
		logx.Infof("shard[%d] replica[%d] insert error:%v, will switch to next replica node", shardClient.ShardIndex, index, err)
	}
	return err
}

func (shardClient *shardConn) ExecAuto(query string, args ...interface{}) error {
	err := shardClient.GetShardConn().Exec(query, args...)
	if err == nil {
		return nil
	}
	logx.Infof("shard[%d] primary node execute error:%v, will switch to replica node", shardClient.ShardIndex, err)
	for i, replicaConn := range shardClient.GetReplicaConn() {
		index := i + 1
		err = replicaConn.Exec(query, args...)
		if err == nil {
			return nil
		}
		logx.Infof("shard[%d] replica[%d] execute error:%v, will switch to next replica node", shardClient.ShardIndex, index, err)
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
