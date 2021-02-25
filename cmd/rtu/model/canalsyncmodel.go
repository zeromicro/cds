package model

import (
	originJson "encoding/json"
	"strconv"
	"time"
	"unsafe"

	"github.com/tal-tech/go-zero/core/logx"
)

type (
	// CanalMysql 新的类型
	CanalMysql struct {
		Before     []ValueMap              `json:"old"`
		Cache      []originJson.RawMessage `json:"data"`
		After      []ValueMap
		Op         string `json:"type"`
		values     []interface{}
		existsKeys []int8
		cacheMap   *ValueMap
	}
)

const (
	canalINSERT = "INSERT"
	canalUPDATE = "UPDATE"
	canalDELETE = "DELETE"
)

// GetOp 获取 opLog/binLog 的操作类型
func (mysql *CanalMysql) GetOp() string {
	return mysql.Op
}

// SetOp 写入opLog/binLog 的类型
func (mysql *CanalMysql) SetOp(s string) {
	mysql.Op = s
}

// SetCacheMap 将json解析成map并放进来
func (mysql *CanalMysql) SetCacheMap(m *ValueMap) {
	mysql.cacheMap = m
}

// GetCacheMap 获取解析好的map数据
func (mysql *CanalMysql) GetCacheMap() *ValueMap {
	return mysql.cacheMap
}

// GetCategory 获取类型，是mysql还是mongo
func (mysql *CanalMysql) GetCategory() string {
	return CANALMYSQL
}

// SetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新
func (mysql *CanalMysql) SetExistsKeys(val []int8) {
	mysql.existsKeys = val
}

// GetExistsKeys 返回用于标记该字段是否存在，主要用于Mongo更新, 0 不存在，1 存在
func (mysql *CanalMysql) GetExistsKeys() []int8 {
	return mysql.existsKeys
}

// ParseToMap 将json解析成map
func (mysql *CanalMysql) ParseToMap(table *SQLTable) (ValueMap, error) {
	if mysql.After == nil {
		return nil, ErrEmptyPayload
	}
	switch mysql.Op {
	case canalINSERT, canalUPDATE:
		if mysql.Op == canalINSERT {
			mysql.Op = "c"
		} else {
			mysql.Op = "u"
		}
		mysql.setValueMap(mysql.After[0], table, false)
		return mysql.After[0], nil
	case canalDELETE:
		mysql.Op = "d"
		mysql.After[0]["ck_is_delete"] = 1
		mysql.setValueMap(mysql.After[0], table, true)
		return mysql.After[0], nil
	default:
		logx.Errorf("not except action: %s", mysql.Op)
		return nil, ErrAction
	}
}

func (mysql *CanalMysql) setValueMap(vm ValueMap, table *SQLTable, isDelete bool) {
	for k, v := range vm {
		if dataType, ok := table.Types[k]; ok {
			if isDelete && k == "ck_is_delete" {
				vm[k] = 1
				continue
			}

			switch dataType {
			case DataTypeFloat:
				switch val := v.(type) {
				case nil:
					vm[k] = float64(0)
				case string:
					tmp, err := strconv.ParseFloat(val, 64)
					if err != nil {
						logx.Errorf("can not format float64 [%v]", v)
						vm[k] = float64(0.0)
						continue
					}
					vm[k] = tmp
				default:
					vm[k] = v
				}
			case DataTypeTime:
				if val, ok := v.(string); ok {
					t := FormatDate(val)
					vm[k] = &t
				} else {
					tmp := NullValMap[DataTypeTime].(time.Time)
					vm[k] = &tmp
				}
			case DataTypeInt:
				switch val := v.(type) {
				case nil:
					vm[k] = 0
				case float64:
					vm[k] = int(val)
				case string:
					i, err := strconv.Atoi(val)
					if err != nil {
						logx.Error(err)
						vm[k] = 0
					} else {
						vm[k] = i
					}
				}
			case DataTypeString:
				if v == nil {
					vm[k] = ""
					continue
				}
				if val, ok := v.(string); ok {
					vm[k] = val
				}
			}
		}
	}
}

// SetValues 存放将插入db的 []interface{}
func (mysql *CanalMysql) SetValues(val []interface{}) {
	mysql.values = val
}

// GetValues 用于获取 准备好插入db的[]interface{}
func (mysql *CanalMysql) GetValues() []interface{} {
	return mysql.values
}

// UnmarshalFromStr UnmarshalFromStr
func (mysql *CanalMysql) UnmarshalFromStr(str string, pool *MapPool) error {
	b := *(*[]byte)(unsafe.Pointer(&str))
	err := originJson.Unmarshal(b, mysql)
	if err != nil {
		return err
	}
	mysql.After = make([]ValueMap, 0, len(mysql.Cache))
	for _, v := range mysql.Cache {
		m := pool.Get()
		err := json.Unmarshal(v, &m)
		if err == nil {
			mysql.After = append(mysql.After, m)
		} else if label, ok := labels[mysql.GetCategory()]; ok && label != nil {
			montorVec.Inc(label)
		}
	}
	return nil
}

// UnmarshalFromStr UnmarshalFromStr
func (mysql *CanalMysql) UnmarshalFromByte(b []byte, pool *MapPool) error {
	err := json.Unmarshal(b, mysql)
	if err != nil {
		if label, ok := labels[mysql.GetCategory()]; ok && label != nil {
			montorVec.Inc(label)
		}
		logx.Error(err)
		return err
	}
	mysql.After = make([]ValueMap, 0, len(mysql.Cache))
	for _, v := range mysql.Cache {
		m := pool.Get()
		err := json.Unmarshal(v, &m)
		if err == nil {
			mysql.After = append(mysql.After, m)
		} else if label, ok := labels[mysql.GetCategory()]; ok && label != nil {
			montorVec.Inc(label)
		}
	}
	return nil
}

// Unpack 展开，防止 canal 用 batch 方式提交
func (mysql *CanalMysql) Unpack() []DataInterface {
	if _, ok := actionWhiteList[mysql.Op]; !ok {
		return []DataInterface{}
	}

	tmp := make([]DataInterface, 0, len(mysql.After))
	for i := 0; i < len(mysql.After); i++ {
		newObj := &CanalMysql{
			After: []ValueMap{mysql.After[i]},
			Op:    mysql.Op,
		}
		tmp = append(tmp, newObj)
	}
	mysql.After = nil
	return tmp
}

var actionWhiteList = map[string]struct{}{
	canalDELETE: {},
	canalINSERT: {},
	canalUPDATE: {},
}
