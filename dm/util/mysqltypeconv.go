package util

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
)

type (
	MysqlTypeConverter struct {
		mysqlConn *sql.DB
	}
	MysqlDesc struct {
		Field   string `db:"Field"`
		Type    string `db:"Type"`
		Null    string `db:"Null"`
		Key     string `db:"Key"`
		Default string `db:"Default"`
		Extra   string `db:"Extra"`
	}
	DataType int
)

const (
	DataTypeString = iota
	DataTypeInt
	DataTypeFloat
	DataTypeTime

	DateTimeLayout = "2006-01-02 15:04:05"
	DateLayout     = "2006-01-02"
)

var ShangHaiLocation = time.FixedZone("Asia/Shanghai", int((time.Hour * 8).Seconds()))

func NewMysqlTypeConvModel(mysqlConn *sql.DB) *MysqlTypeConverter {
	return &MysqlTypeConverter{mysqlConn: mysqlConn}
}

func (tc *MysqlTypeConverter) ObtainMysqlTypeMap(tableName string) map[string]DataType {
	mp := make(map[string]string)
	res := make(map[string]DataType)
	descSQL := "DESC `" + tableName + "`"
	rows, err := tc.mysqlConn.Query(descSQL)
	if err != nil {
		logx.Error(err)
	}
	for rows.Next() {
		var mysqlDesc MysqlDesc
		err := rows.Scan(&mysqlDesc.Field, &mysqlDesc.Type, &mysqlDesc.Null, &mysqlDesc.Key, &mysqlDesc.Default, &mysqlDesc.Extra)
		if err != nil {
			logx.Error(err)
		}
		mp[mysqlDesc.Field] = mysqlDesc.Type
	}
	for k, v := range mp {
		d := ParseTypeByMysqlType(v)
		res[k] = d
	}
	return res
}

func ParseValueByType(v string, t DataType) (interface{}, error) {
	if v == "<nil>" {
		return nil, nil
	}
	switch t {
	case DataTypeInt:
		if v == "" {
			v = "0"
		}
		return strconv.ParseInt(v, 10, 64)
	case DataTypeFloat:
		if v == "" {
			v = "0"
		}
		f, e := strconv.ParseFloat(v, 64)
		if e != nil {
			return nil, e
		}
		return f, nil
	case DataTypeTime:
		s := strings.Trim(v, "\"")
		if s == "0000-00-00 00:00:00" || s == "0000-00-00" || len(s) == 0 {
			return time.Time{}, nil
		}
		if len(s) < len(DateTimeLayout) {
			if len(s) < len(DateLayout) {
				return nil, errors.New("bad time format:" + s)
			}
			tim, e := time.ParseInLocation(DateLayout, s[:len(DateLayout)], ShangHaiLocation)
			if e != nil {
				logx.Error(e)
				return nil, e
			}
			return tim, nil
		}
		tim, e := time.ParseInLocation(DateTimeLayout, s[:len(DateTimeLayout)], ShangHaiLocation)
		if e != nil {
			return nil, e
		}
		return tim, nil
	default:
		out := ""
		e := json.Unmarshal([]byte(v), &out)
		if e != nil {
			return TrimBoth(v, "\""), nil
		}
		return out, nil
	}
}

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

func TrimStart(s, trim string) string {
	if strings.HasPrefix(s, trim) {
		return s[len(trim):]
	}
	return s
}

func TrimEnd(s, trim string) string {
	if strings.HasSuffix(s, trim) {
		return s[:len(s)-len(trim)]
	}
	return s
}

func TrimBoth(s, trim string) string {
	return TrimStart(TrimEnd(s, trim), trim)
}
