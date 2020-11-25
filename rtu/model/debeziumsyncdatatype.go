package model

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DataType 用于Go内部转换的数据类型
type DataType int

// supported data type
const (
	DataTypeString = iota
	DataTypeInt
	DataTypeFloat
	DataTypeTime
)

var (
	// NullValMap 记录该类型的默认值
	NullValMap = map[DataType]interface{}{
		DataTypeString: "",
		DataTypeInt:    0,
		DataTypeFloat:  0.0,
		DataTypeTime:   time.Unix(0, 0),
	}
)

// ParseTypeByMysqlType 将MySQL的数据类型转换为Go语言内部转换用的DataType
func ParseTypeByMysqlType(sqlType string) DataType {
	sqlType = strings.ToLower(sqlType)
	if strings.Contains(sqlType, "int") {
		return DataTypeInt
	}
	if strings.Contains(sqlType, "decimal") || strings.Contains(sqlType, "double") || strings.Contains(sqlType, "float") {
		return DataTypeFloat
	}
	if strings.Contains(sqlType, "date") || strings.Contains(sqlType, "time") {
		return DataTypeTime
	}
	return DataTypeString
}

// ParseTypeByCkType 解析类型到ch的类型
func ParseTypeByCkType(ckType string) DataType {
	s := strings.ToLower(ckType)
	if strings.Contains(s, "int") || strings.Contains(s, "boolean") {
		return DataTypeInt
	}
	if strings.Contains(s, "float") || strings.Contains(s, "decimal") {
		return DataTypeFloat
	}
	if strings.Contains(s, "date") {
		return DataTypeTime
	}
	return DataTypeString
}

// Data 关系型数据的一条记录
type Data struct {
	Keys                  []string
	Values                []interface{} //不用map是因为数据量少的情况下，slice性能更高
	CheckPoint, Operation string
}

// SQLDateTimeLayout SQLDateTimeLayout
const (
	SQLDateCanalTimeLayout = "2006-01-02 15:04:05"
	MySQLTimeLayout        = "2006-01-02T15:04:05Z"
	DateTimeLayout         = "2006-01-02"
)

// NewData 新建Data
func NewData() *Data {
	return &Data{
		Keys:   make([]string, 0, 16),
		Values: make([]interface{}, 0, 16),
	}
}

// Set 设置键值
func (s *Data) Set(k string, v interface{}) {
	for i := 0; i < len(s.Keys); i++ {
		if s.Keys[i] == k {
			s.Values[i] = v
			return
		}
	}
	s.Keys = append(s.Keys, k)
	s.Values = append(s.Values, v)
}

// Get 获取键值
func (s *Data) Get(k string) interface{} {
	for i := 0; i < len(s.Keys); i++ {
		if s.Keys[i] == k {
			return s.Values[i]
		}
	}
	return nil
}

// Exists 判断键是否存在
func (s *Data) Exists(k string) bool {
	for _, key := range s.Keys {
		if key == k {
			return true
		}
	}
	return false
}

// String 转换成string，方便打印
func (s *Data) String() string {
	return fmt.Sprint(s.Values)
}

// GetValues 获取指定键的值
func (s *Data) GetValues(keys []string) []interface{} {
	out := []interface{}{}
	for _, key := range keys {
		out = append(out, s.Get(key))
	}
	return out
}

// ParseSQLValueByType 按类型
func ParseSQLValueByType(typ DataType, str string) (interface{}, error) {
	if str == "NULL" {
		return nil, nil
	}
	switch typ {
	case DataTypeInt:
		return strconv.ParseInt(str, 10, 64)
	case DataTypeFloat:
		f, e := strconv.ParseFloat(str, 64)
		if e != nil {
			return nil, e
		}
		return f, nil
	case DataTypeTime:
		if len(str) < len(SQLDateCanalTimeLayout) {
			return nil, errors.New("invalid time format:" + str)
		}
		t, e := time.Parse(SQLDateCanalTimeLayout+" MST", strings.Replace(str[:len(SQLDateCanalTimeLayout)], "T", " ", -1)+" CST")
		if e != nil {
			return nil, e
		}
		return t, nil
	default:
		return str, nil
	}
}

func FormatDate(timeStr string) time.Time {
	t, err := time.ParseInLocation(DateTimeLayout, timeStr, time.Local)
	if err == nil {
		return t
	}
	t, err = time.ParseInLocation(MySQLTimeLayout, timeStr, time.Local)
	if err == nil {
		return t
	}
	t, err = time.ParseInLocation(SQLDateCanalTimeLayout, timeStr, time.Local)
	if err == nil {
		return t
	}
	return NullValMap[DataTypeTime].(time.Time)
}
