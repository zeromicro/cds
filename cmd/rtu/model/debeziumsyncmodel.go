package model

import (
	"strconv"
	"strings"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
)

// GetOp 获取 opLog/binLog 的操作类型
func (mysql *DebeziumMySQL) GetOp() string {
	if mysql.Payload == nil {
		return ""
	}
	return mysql.Payload.Op
}

// GetOp 获取 opLog/binLog 的操作类型
func (mongo *DebeziumMongo) GetOp() string {
	if mongo.Payload == nil {
		return ""
	}
	return mongo.Payload.Op
}

// SetOp 写入opLog/binLog 的类型
func (mysql *DebeziumMySQL) SetOp(s string) {
	mysql.Payload.Op = s
}

// SetOp 写入opLog/binLog 的类型
func (mongo *DebeziumMongo) SetOp(s string) {
	mongo.Payload.Op = s
}

// SetCacheMap 将json解析成map并放进来
func (mysql *DebeziumMySQL) SetCacheMap(m *ValueMap) {
	mysql.cacheMap = m
}

// SetCacheMap 将json解析成map并放进来
func (mongo *DebeziumMongo) SetCacheMap(m *ValueMap) {
	mongo.cacheMap = m
}

// GetCacheMap 获取解析好的map数据
func (mysql *DebeziumMySQL) GetCacheMap() *ValueMap {
	return mysql.cacheMap
}

// GetCacheMap 获取解析好的map数据
func (mongo *DebeziumMongo) GetCacheMap() *ValueMap {
	return mongo.cacheMap
}

// GetCategory 获取类型，是mysql还是mongo
func (mysql *DebeziumMySQL) GetCategory() string {
	return DBZUMMYSQL
}

// GetCategory 获取类型，是mysql还是mongo
func (mongo *DebeziumMongo) GetCategory() string {
	return DBZUMMONGO
}

// SetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新
func (mysql *DebeziumMySQL) SetExistsKeys(val []int8) {
	mysql.existsKeys = val
}

// SetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新
func (mongo *DebeziumMongo) SetExistsKeys(val []int8) {
	mongo.existsKeys = val
}

// GetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新, 0 不存在，1 存在
func (mysql *DebeziumMySQL) GetExistsKeys() []int8 {
	return mysql.existsKeys
}

// GetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新, 0 不存在，1 存在
func (mongo *DebeziumMongo) GetExistsKeys() []int8 {
	return mongo.existsKeys
}

// ParseToMap 将json解析成map
func (mysql *DebeziumMySQL) ParseToMap(table *SQLTable) (ValueMap, error) {
	if mysql.Payload == nil {
		return nil, nil
	}
	switch mysql.Payload.Op {
	case "c", "u":
		mysql.setValueMap(mysql.Payload.After, table, false)
		return mysql.Payload.After, nil
	case "d":
		mysql.setValueMap(mysql.Payload.Before, table, true)
		return mysql.Payload.Before, nil
	default:
		logx.Errorf("not except action: %s", mysql.Payload.Op)
		return nil, ErrAction
	}
}

// ParseToMap 将json解析成map
func (mongo *DebeziumMongo) ParseToMap(table *SQLTable) (ValueMap, error) {
	if mongo.Payload == nil {
		return nil, nil
	}
	switch mongo.Payload.Op {
	case "c":
		tmp := ValueMap{}
		err := json.UnmarshalFromString(mongo.Payload.After, &tmp)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		result := ValueMap{}
		mongo.setValueMap(result, tmp, table)
		return result, nil
	case "u":
		tmp := ValueMap{}
		err := json.UnmarshalFromString(mongo.Payload.Patch, &tmp)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		result := ValueMap{}
		mongo.setValueMap(result, tmp, table)

		tmp = ValueMap{}
		err = json.UnmarshalFromString(mongo.Payload.Filter, &tmp)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		mongo.setValueMap(result, tmp, table)
		return result, nil
	case "d":
		tmp := ValueMap{}
		err := json.UnmarshalFromString(mongo.Payload.Filter, &tmp)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		result := ValueMap{}

		mongo.setValueMap(result, tmp, table)
		return result, nil
	case "r":
		return nil, nil
	default:
		logx.Errorf("not except action: %s", mongo.Payload.Op)
		return nil, ErrAction
	}
}

func (mongo *DebeziumMongo) setValueMap(vm, tmp ValueMap, table *SQLTable) {
	for k, v := range tmp {
		switch v := v.(type) {
		case float64, string:
			if k == "$v" {
				continue
			}
			vm[k] = v
		case map[string]interface{}:
			switch k {
			case "_id":
				// 插入/更新的主键
				for idk, idv := range v {
					tmpIdk := strings.ReplaceAll(idk, "$", "")
					if tmpIdk == "oid" {
						vm["_id"] = idv
					} else {
						vm[tmpIdk] = idv
					}
				}
			case "$set":
				// 更新的具体内容
				for idk, idv := range v {
					vm[strings.ReplaceAll(idk, "$", "")] = idv
				}
			default:
				res, err := json.Marshal(v)
				if err != nil {
					logx.Error(err)
				}
				vm[k] = string(res)
			}
		}
	}

	for k, v := range vm {
		if dataType, ok := table.Types[k]; ok {
			switch dataType {
			case DataTypeTime:
				if val, ok := v.(map[string]interface{}); ok {
					if ts, ok := val["$date"]; ok {
						t := int64(ts.(float64)) / 1000
						ms := int64(ts.(float64)) - t*1000
						tm := time.Unix(t, ms)
						vm[k] = &tm
					}
					continue

				}
				if val, ok := v.(string); ok {
					ts := int64(json.Get([]byte(val), "$date").ToInt())
					tm := time.Unix(ts/1000, 0)
					vm[k] = &tm

					continue

				}
			case DataTypeInt:
				switch v := v.(type) {
				case float64:
					vm[k] = int(v)
				case string:
					i, err := strconv.Atoi(v)
					if err != nil {
						logx.Error(err)
						vm[k] = 0
					} else {
						vm[k] = i
					}
				}
			}
		}
	}
}

func (mysql *DebeziumMySQL) setValueMap(vm ValueMap, table *SQLTable, isDelete bool) {
	for k, v := range vm {
		if dataType, ok := table.Types[k]; ok {
			if isDelete && k != table.PrimaryKey {
				vm[k] = NullValMap[dataType]
				continue
			}
			switch dataType {
			case DataTypeTime:
				if val, ok := v.(string); ok {
					t, err := time.Parse(MySQLTimeLayout, val)
					if err != nil {
						t = NullValMap[DataTypeTime].(time.Time)
					}
					vm[k] = t

				}
			case DataTypeInt:
				switch v := v.(type) {
				case float64:
					vm[k] = int(v)
				case string:
					i, err := strconv.Atoi(v)
					if err != nil {
						logx.Error(err)
						vm[k] = 0
					} else {
						vm[k] = i
					}
				}
			}
		}
	}
}

// SetValues 存放将插入db的 []interface{}
func (mysql *DebeziumMySQL) SetValues(val []interface{}) {
	mysql.values = val
}

// SetValues 存放将插入db的 []interface{}
func (mongo *DebeziumMongo) SetValues(val []interface{}) {
	mongo.values = val
}

// GetValues 用于获取 准备好插入db的[]interface{}
func (mysql *DebeziumMySQL) GetValues() []interface{} {
	return mysql.values
}

// GetValues 用于获取 准备好插入db的[]interface{}
func (mongo *DebeziumMongo) GetValues() []interface{} {
	return mongo.values
}

// UnmarshalFromStr UnmarshalFromStr
func (mongo *DebeziumMongo) UnmarshalFromStr(str string, mp *MapPool) error {
	return json.UnmarshalFromString(str, mongo)
}

// UnmarshalFromByte UnmarshalFromStr
func (mongo *DebeziumMongo) UnmarshalFromByte(b []byte, mp *MapPool) error {
	return json.Unmarshal(b, mongo)
}

// Unpack Unpack
func (mongo *DebeziumMongo) Unpack() []DataInterface {
	return []DataInterface{mongo}
}

// UnmarshalFromByte UnmarshalFromStr
func (mysql *DebeziumMySQL) UnmarshalFromByte(b []byte, mp *MapPool) error {
	return json.Unmarshal(b, mysql)
}

// Unpack Unpack
func (mysql *DebeziumMySQL) Unpack() []DataInterface {
	return []DataInterface{mysql}
}

// UnmarshalFromStr UnmarshalFromStr
func (mysql *DebeziumMySQL) UnmarshalFromStr(str string, mp *MapPool) error {
	return json.UnmarshalFromString(str, mysql)
}
