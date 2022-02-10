package ckgroup

import (
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

var aliveOnce = sync.Once{}

func (g *dbGroup) KeepAlive(intervalSecond int) {
	aliveOnce.Do(func() {
		g.pingWithInterval(intervalSecond)
	})
}

func (g *dbGroup) pingWithInterval(intervalSecond int) {
	go func() {
		for {
			time.Sleep(time.Second * time.Duration(intervalSecond))
			g.ping()
		}
	}()
}

func (g *dbGroup) ping() {
	for i, shard := range g.ShardNodes {
		err := shard.GetShardConn().GetRawConn().Ping()
		if err != nil {
			logx.Errorf("ping shard:%d error:%v", i+1, err)
		}
		for j, replica := range shard.GetReplicaConn() {
			err := replica.GetRawConn().Ping()
			if err != nil {
				logx.Errorf("ping shard:%d replica:%d error:%v", i+1, j+1, err)
			}
		}
	}
}
