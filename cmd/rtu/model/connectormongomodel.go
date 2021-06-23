package model

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/tal-tech/go-zero/core/logx"
)

type (
	// ConnectorMongo mongo type of connector
	ConnectorMongo struct {
		values            []interface{}
		existsKeys        []int8
		cacheMap          *ValueMap
		Payload           ValueMap `json:"fullDocument"`
		DocumentKey       ValueMap `json:"documentKey"`
		UpdateDescription struct {
			UpdatedFields ValueMap `json:"updatedFields"`
			RemovedFields []string `json:"removedFields"`
		} `json:"updateDescription"`
		op            string
		OperationType string `json:"operationType"`

		FullDocument ValueMap
	}
)

// GetOp 获取操作类型
func (mongo *ConnectorMongo) GetOp() string {
	if mongo == nil {
		return ""
	}
	return mongo.op
}

// SetOp 写入opLog/binLog 的类型
func (mongo *ConnectorMongo) SetOp(s string) {
	mongo.op = s
}

// SetCacheMap 将json解析成map并放进来
func (mongo *ConnectorMongo) SetCacheMap(m *ValueMap) {
	mongo.cacheMap = m
}

// GetCacheMap 获取解析好的map数据
func (mongo *ConnectorMongo) GetCacheMap() *ValueMap {
	return mongo.cacheMap
}

// GetCategory 获取类型，是mysql还是mongo
func (mongo *ConnectorMongo) GetCategory() string {
	return CONNMONGO
}

// SetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新
func (mongo *ConnectorMongo) SetExistsKeys(val []int8) {
	mongo.existsKeys = val
}

// GetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新, 0 不存在，1 存在
func (mongo *ConnectorMongo) GetExistsKeys() []int8 {
	return mongo.existsKeys
}

// ParseToMap 将json解析成map
func (mongo *ConnectorMongo) ParseToMap(table *SQLTable) (ValueMap, error) {

	if mongo.Payload == nil && mongo.DocumentKey == nil {
		logx.Error("has no mongo.Payload && mongo.DocumentKey")
		return nil, nil
	}
	switch mongo.OperationType {
	case "insert":
		mongo.op = "c"
		mongo.setValueMap(mongo.Payload, table)
		return mongo.Payload, nil
	case "replace":
		mongo.op = "u"
		mongo.setValueMap(mongo.Payload, table)
		return mongo.Payload, nil
	case "update":
		if mongo.Payload != nil {
			mongo.setValueMap(mongo.Payload, table)
			return mongo.Payload, nil
		}
		mongo.setValueMap(mongo.DocumentKey, table)
		mongo.setUpdateValueMap(mongo.DocumentKey, table)
		mongo.op = "u"
		return mongo.DocumentKey, nil
	case "delete":
		mongo.setValueMap(mongo.DocumentKey, table)
		mongo.DocumentKey["ck_is_delete"] = 1
		mongo.op = "d"

		return mongo.DocumentKey, nil
	default:
		logx.Errorf("not except action: %s", mongo.OperationType)
		return nil, ErrAction
	}
}

func (mongo *ConnectorMongo) setUpdateValueMap(vm ValueMap, table *SQLTable) {
	updateDesc := mongo.UpdateDescription

	if len(updateDesc.UpdatedFields) != 0 {
		mongo.setValueMap(updateDesc.UpdatedFields, table)
		for k, v := range updateDesc.UpdatedFields {
			vm[k] = v
		}
	}

	for _, columnName := range updateDesc.RemovedFields {
		if dataType, exist := table.Types[columnName]; exist {
			vm[columnName] = NullValMap[dataType]
		}
	}
}

func (mongo *ConnectorMongo) setValueMap(vm ValueMap, table *SQLTable) {
	// 首先将嵌套的map展平
	for k, v := range vm {
		switch v.(type) {
		case map[string]interface{}:
			if k == "_id" {
				tmp := vm[k].(map[string]interface{})["$oid"]
				if val, ok := tmp.(string); ok {
					vm[k] = val
				}
			} else {
				tmp := vm[k].(map[string]interface{})
				// 是否日期类型, {u'$date': 1592906230494}
				if val, ok := tmp["$date"]; ok {
					switch typev := val.(type) {
					case float64:
						vm[k] = int(typev)
					case int:
						vm[k] = typev
					case string:
						tmp := typev
						if len(tmp) == 0 {
							vm[k] = 0
						} else {
							r, err := strconv.Atoi(tmp)
							if err != nil {
								logx.Errorf("format string to int err, %v", tmp)
								panic("error in format")
							}
							vm[k] = r
						}

					default:
						logx.Errorf("format string to int err, %v, type is %v", tmp, reflect.TypeOf(val))
						panic("error in format")
					}
				} else
				// 是否 numberLong类型, {u'$numberLong': u'1594655999000'}
				if val, ok := tmp["$numberLong"]; ok {
					if str, ok := val.(string); ok {
						result, err := strconv.Atoi(str)
						if err != nil {
							result = 0
						}
						vm[k] = result
					}
				}
			}
		case int, float64, string:
		default:
			tmp, err := json.Marshal(v)
			if err != nil {
				logx.Error(err)
				delete(vm, k)
				continue
			}
			vm[k] = string(tmp)
		}

		if dataType, ok := table.Types[k]; ok {
			switch dataType {
			case DataTypeTime:
				switch v := vm[k].(type) {
				case int:
					val := v
					t, err := formatTime(val)
					if err != nil {
						panic(fmt.Sprintf("time format error. val is [%d]", val))
					}
					vm[k] = t
				case string:
					tmp := v
					t, err := formatTimeWithLayout(tmp)
					if err != nil {
						panic(fmt.Sprintf("time format error. val is [%s]", tmp))
					}
					vm[k] = t
				default:
					logx.Error(vm)
					logx.Error(reflect.TypeOf(vm[k]))
					panic("err")
				}
			case DataTypeString:
				if _, ok := vm[k]; !ok {
					vm[k] = ""
					continue
				}
				if val, ok := vm[k].(float64); ok {
					tmp := int(val)
					vm[k] = strconv.Itoa(tmp)
					continue
				}
				if val, ok := vm[k].(map[string]interface{}); ok {
					tmp, err := json.Marshal(val)
					if err != nil {
						logx.Error(err)
						delete(vm, k)
						continue
					}
					vm[k] = string(tmp)
					continue
				}
				if _, ok := vm[k].(string); !ok {
					logx.Infof("table: %s can't format, type is %s, table type is: string", table.Table, reflect.TypeOf(vm[k]))
					continue
				}
			case DataTypeInt:
				if val, ok := vm[k].(float64); ok {
					vm[k] = int(val)
					continue
				}
				if val, ok := vm[k].(string); ok {
					tmp, err := strconv.Atoi(val)
					if err != nil {
						logx.Error(err)
					}
					vm[k] = tmp
				}
			}
		}
	}
}

// SetValues 存放将插入db的 []interface{}
func (mongo *ConnectorMongo) SetValues(val []interface{}) {
	mongo.values = val
}

// GetValues 用于获取 准备好插入db的[]interface{}
func (mongo *ConnectorMongo) GetValues() []interface{} {
	return mongo.values
}

// UnmarshalFromStr UnmarshalFromStr
func (mongo *ConnectorMongo) UnmarshalFromStr(str string, mp *MapPool) error {
	tmp := new(string)
	err := json.UnmarshalFromString(str, tmp)
	if err != nil {
		return err
	}
	return json.UnmarshalFromString(*tmp, mongo)
}

// Unpack Unpack
func (mongo *ConnectorMongo) Unpack() []DataInterface {
	return []DataInterface{mongo}
}

// UnmarshalFromByte UnmarshalFromStr
func (mongo *ConnectorMongo) UnmarshalFromByte(b []byte, mp *MapPool) error {
	tmp := new(string)
	err := json.Unmarshal(b, tmp)
	if err != nil {
		return err
	}
	err = json.UnmarshalFromString(*tmp, mongo)
	if err != nil {
		return err
	}
	if len(mongo.OperationType) != 0 {
		return nil
	}
	m := mp.Get()
	err = json.UnmarshalFromString(*tmp, &m)
	if err != nil {
		return err
	}
	mongo.OperationType = "insert"
	mongo.Payload = m
	return nil
}
