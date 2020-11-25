package ckgroup

import (
	"errors"
	"sort"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/tal-tech/go-zero/core/logx"

	"cds/tools/ckgroup/config"
)

type (
	DBGroup interface {
		GetQueryNode() CKConn
		GetAllNodes() []CKConn
		GetAllShard() []ShardConn

		// Deprecated: Use CKConn.QueryStream instead.
		BatchQueryRows(v interface{}, cnt int, query string, args ...interface{}) (chan interface{}, error)
		KeepAlive(intervalSecond int)

		// InsertAuto 自动把数组内的数据根据 siphash 分片插入到各个 clickhouse 节点
		// query  形如 insert into user (id,real_name,city) values (#{id},#{real_name},#{city}) . #{}内的字符只能是大小写字母,数字和下划线
		// hashTag  struct 分片字段 `db` tag 的值
		// sliceData  要输入的数组 , 类型只能是 []*sturct 或 []struct
		InsertAuto(query string, hashTag string, sliceData interface{}) error
		ExecAuto(query string, hashIdx int, args [][]interface{}) error
		ExecAll(query string, args [][]interface{}) error
		Close()
	}
	dbGroup struct {
		ShardNodes []ShardConn
		QueryNode  CKConn
		opt        option
	}

	rowValue = []interface{}
)

func NewCKGroup(c config.Config, opts ...OptionFunc) (DBGroup, error) {
	dbGroup := dbGroup{opt: newOptions(opts...)}

	// 退出时清理，当报错退出时，关闭所有的连接
	isClean := false
	defer func() {
		if isClean {
			dbGroup.Close()
		}
	}()

	sort.SliceStable(c.ShardGroups, func(i, j int) bool { // must keep stable ordering
		return c.ShardGroups[i].ShardNode < c.ShardGroups[j].ShardNode
	})

	for i, item := range c.ShardGroups {
		shardConn, err := NewShardConn(i+1, item)
		if err != nil {
			isClean = true
			return nil, err
		}
		dbGroup.ShardNodes = append(dbGroup.ShardNodes, shardConn)
	}
	if c.QueryNode == "" {
		if len(dbGroup.ShardNodes) == 0 {
			return nil, errors.New("ckgroup config is error")
		}
		dbGroup.QueryNode = dbGroup.ShardNodes[0].GetShardConn()
	} else {
		queryNode, err := NewCKConn(c.QueryNode)
		if err != nil {
			isClean = true
			return nil, err
		}
		dbGroup.QueryNode = queryNode
	}
	return &dbGroup, nil
}

func MustCKGroup(c config.Config, opts ...OptionFunc) DBGroup {
	group, err := NewCKGroup(c, opts...)
	panicIfErr(err)
	return group
}

func (g *dbGroup) GetAllNodes() []CKConn {
	var all []CKConn
	for _, shard := range g.ShardNodes {
		all = append(all, shard.GetAllConn()...)
	}
	return all
}
func (g *dbGroup) GetAllShard() []ShardConn {
	return g.ShardNodes
}

func (g *dbGroup) GetQueryNode() CKConn {
	return g.QueryNode
}

func (g *dbGroup) Close() {
	if g.QueryNode != nil {
		if err := g.QueryNode.GetRawConn().Close(); err != nil {
			logx.Error(err)
		}
	}
	for _, shard := range g.ShardNodes {
		shard.Close()
	}
}

// Deprecated: Use QueryStream instead.
func (g *dbGroup) BatchQueryRows(v interface{}, cnt int, query string, args ...interface{}) (chan interface{}, error) {
	// 通过管道返回查询结果，实现类似流，边输出边处理
	ch := make(chan interface{}, cnt)
	err := BatchScanRows(g.QueryNode.GetRawConn(), ch, v, query, args...)
	if err != nil {
		return nil, err
	}
	return ch, nil
}
