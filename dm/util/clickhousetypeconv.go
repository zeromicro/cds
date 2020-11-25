package util

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
)

func RepairData(mp map[string]interface{}, name2Type map[string]string) ([]interface{}, string, int, error) {
	defaultTypeMap := getDefaultValue(name2Type)
	for k := range defaultTypeMap {
		if mp[k] != nil {
			defaultTypeMap[k] = CorrectColumn(mp[k], name2Type[k])
		}
	}

	kt := make([]string, 0, len(defaultTypeMap))
	for k := range defaultTypeMap {
		kt = append(kt, k)
	}
	sort.Strings(kt)

	pic, tp, id, err := convertBsonType2GoType(kt, defaultTypeMap)
	if err != nil {
		return nil, "", 0, err
	}
	return pic, tp, id, nil
}

//根据desc clickhouse的结果制造 字段名2默认值映射
func getDefaultValue(name2Type map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(name2Type))
	for k, v := range name2Type {
		v := strings.ToLower(v)
		switch {
		case strings.Contains(v, "int") || strings.Contains(v, "bool"):
			result[k] = 0
		case strings.Contains(v, "string"):
			result[k] = ""
		case strings.Contains(v, "date"):
			result[k] = time.Time{}
		case strings.Contains(v, "float"):
			result[k] = 0.0
		case strings.Contains(v, "array"):
			result[k] = make([]interface{}, 0)
		}
	}
	return result
}

func CorrectColumn(source interface{}, tp string) interface{} {
	tp = strings.ToLower(tp)
	switch {
	case strings.Contains(tp, "float64") && reflect.TypeOf(source).Kind() == reflect.Int32:
		if temp, ok := source.(int32); ok {
			source = float64(temp)
		}
	case strings.Contains(tp, "float32") && reflect.TypeOf(source).Kind() == reflect.Int32:
		if temp, ok := source.(int32); ok {
			source = float32(temp)
		}
	case strings.Contains(tp, "float64") && reflect.TypeOf(source).Kind() == reflect.Int64:
		if temp, ok := source.(int64); ok {
			source = float64(temp)
		}
	case strings.Contains(tp, "float32") && reflect.TypeOf(source).Kind() == reflect.Int64:
		if temp, ok := source.(int64); ok {
			source = float32(temp)
		}
	case strings.Contains(tp, "int32") && reflect.TypeOf(source).Kind() == reflect.Float32:
		if temp, ok := source.(float32); ok {
			source = int32(temp)
		}
	case strings.Contains(tp, "int32") && reflect.TypeOf(source).Kind() == reflect.Float64:
		if temp, ok := source.(float64); ok {
			source = int32(temp)
		}
	case strings.Contains(tp, "int64") && reflect.TypeOf(source).Kind() == reflect.Float32:
		if temp, ok := source.(float32); ok {
			source = int64(temp)
		}
	case strings.Contains(tp, "int64") && reflect.TypeOf(source).Kind() == reflect.Float64:
		if temp, ok := source.(float64); ok {
			source = int64(temp)
		}
	case strings.Contains(tp, "int64") && reflect.TypeOf(source).Kind() == reflect.Int32:
		if temp, ok := source.(int32); ok {
			source = int64(temp)
		}
	case strings.Contains(tp, "int64") && reflect.TypeOf(source).Kind() == reflect.String:
		if temp, ok := source.(string); ok {
			res, err := strconv.ParseInt(temp, 10, 64)
			if err != nil {
				logx.Error(err)
				return err
			}
			source = res
		}
	}
	return source
}
