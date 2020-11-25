package model

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"cds/rtu/monitor"

	jsoniter "github.com/json-iterator/go"
)

const (
	DBZUMMYSQL = "debezium-mysql"
	DBZUMMONGO = "debezium-mongodb"
	CANALMYSQL = "canal-mysql"
	CONNMONGO  = "connector-mongodb"

	timeLayout  = "2006-01-02 15:04:05"
	timeLayout2 = "2006-01-02 15:04:05.000"
)

var (
	// ErrAction 错误的类型信息
	ErrAction = errors.New("not except action")
	// ErrEmptyPayload
	ErrEmptyPayload = errors.New("payload is empty")

	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

var (
	montorVec = monitor.NewUnmarshalVec("datateam", "rtu", "unmarshal")
	labels    = map[string]*monitor.UnmarshalLabels{
		CONNMONGO:  {Category: CONNMONGO, Status: "failed"},
		DBZUMMONGO: {Category: DBZUMMONGO, Status: "failed"},
		DBZUMMYSQL: {Category: DBZUMMYSQL, Status: "failed"},
		CANALMYSQL: {Category: CANALMYSQL, Status: "failed"},
	}
)

type (
	// DataInterface mysql 和 mongo 的封装
	DataInterface interface {
		GetCategory() string
		GetOp() string
		SetOp(s string)

		SetValues(val []interface{})
		GetValues() []interface{}
		ParseToMap(table *SQLTable) (ValueMap, error)
		SetExistsKeys([]int8)
		GetExistsKeys() []int8
		SetCacheMap(m *ValueMap)
		GetCacheMap() *ValueMap

		UnmarshalFromStr(str string, mappool *MapPool) error
		UnmarshalFromByte(b []byte, mappool *MapPool) error

		Unpack() []DataInterface
	}
)

type (
	// DebeziumMySQL 类型
	DebeziumMySQL struct {
		Payload    *debeziumMysqlPayload `json:"payload"`
		values     []interface{}
		existsKeys []int8
		cacheMap   *ValueMap
	}

	// DebeziumMongo 类型
	DebeziumMongo struct {
		Payload    *debeziumMongoPayload `json:"payload"`
		values     []interface{}
		existsKeys []int8
		cacheMap   *ValueMap
	}

	debeziumMysqlPayload struct {
		Before ValueMap `json:"before"`
		After  ValueMap `json:"after"`
		Op     string   `json:"op"`
	}
	debeziumMongoPayload struct {
		Filter string `json:"filter"`
		After  string `json:"after"`
		Patch  string `json:"patch"`
		Op     string `json:"op"`
	}

	// ValueMap 存储的解析出来的map, k 是字段名，v是数据
	ValueMap map[string]interface{}
)

type (
	// MapPool map pool
	MapPool struct {
		p sync.Pool
	}
)

// Get 获取新的 ValueMap
func (p *MapPool) Get() ValueMap {
	m := p.p.Get().(ValueMap)

	return m
}

// Put 放入不用的 ValueMap
func (p *MapPool) Put(m ValueMap) {
	for k := range m {
		delete(m, k)
	}
	p.p.Put(m)
}

// NewMapPool 生成新的 MapPool
func NewMapPool() *MapPool {
	return &MapPool{p: sync.Pool{New: func() interface{} {
		return make(ValueMap, 20)
	}}}
}

func formatTime(ti int) (*time.Time, error) {
	t := int64(ti)

	if t > 1e12 {
		sec := t / 1000
		t := time.Unix(sec, t-sec*1000)
		return &t, nil
	} else {
		t := time.Unix(t, 0)
		return &t, nil
	}
}

func formatTimeWithLayout(dt string) (*time.Time, error) {
	v, err := strconv.Atoi(dt)
	if err != nil {
		return formatTime(v)
	}
	if t, err := time.ParseInLocation(timeLayout, dt, time.Local); err != nil {
		return &t, nil
	}
	if t, err := time.ParseInLocation(timeLayout2, dt, time.Local); err != nil {
		return &t, nil
	} else {
		return nil, err
	}

}
