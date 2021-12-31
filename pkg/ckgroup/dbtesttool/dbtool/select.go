package dbtool

import (
	"fmt"
	"math"

	"github.com/go-xorm/builder"
)

const (
	dataSame = iota
	dataNotSame
	dataLost
)

const step = 1000

func (s *DBTestToolSqlConn) Select(dataSet []*DataInstance) (map[int64]int, error) {
	var tableName string
	if s.dbType == dbTypeMySQL {
		tableName = "test.test_data"
	} else if s.dbType == dbTypeCK {
		tableName = "test.test_data_all"
	}

	result := make(map[int64]int)
	origin := make(map[int64]*DataInstance)
	for _, item := range dataSet {
		result[item.PK] = dataLost
		origin[item.PK] = item
	}
	for i := 0; i < len(dataSet); i += step {
		startIdx := i
		endIdx := startIdx + step
		if endIdx > len(dataSet) {
			endIdx = len(dataSet)
		}
		var ids []int64
		for _, item := range dataSet[startIdx:endIdx] {
			ids = append(ids, item.PK)
		}

		query, args, _ := builder.Select("pk", "int_value", "float_value", "double_value", "char_value", "varchar_value", "time_value").
			From(tableName).Where(builder.In("pk", ids)).ToSQL()
		var queryResult []*DataInstance
		err := s.db.QueryRows(&queryResult, query, args...)
		if err != nil {
			return result, err
		}
		for _, item := range queryResult {
			if Compare(item, origin[item.PK], true) {
				result[item.PK] = dataSame
			} else {
				result[item.PK] = dataNotSame
			}
		}
	}
	return result, nil
}

func Compare(a, b *DataInstance, showDiff bool) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if a == nil && b == nil {
		return true
	}
	if a.PK != b.PK {
		if showDiff {
			fmt.Println("[Diff] pk:", a.PK, b.PK)
		}
		return false
	}
	if a.IntValue != b.IntValue {
		if showDiff {
			fmt.Println("[Diff] IntValue:", a.IntValue, b.IntValue)
		}
		return false
	}
	if math.Abs((float64)(a.FloatValue-b.FloatValue)) > 10e-5 {
		if showDiff {
			fmt.Printf("[Diff] FloatValue: %f %f %f\n", a.FloatValue, b.FloatValue, a.FloatValue-b.FloatValue)
		}
		return false
	}
	if a.DoubleValue != b.DoubleValue {
		if showDiff {
			fmt.Println("[Diff] DoubleValue:", a.DoubleValue, b.DoubleValue)
		}
		return false
	}
	if a.CharValue != b.CharValue {
		if showDiff {
			fmt.Println("[Diff] CharValue:", a.CharValue, b.CharValue)
		}
		return false
	}
	if a.VarCharValue != b.VarCharValue {
		if showDiff {
			fmt.Println("[Diff] VarCharValue:", a.VarCharValue, b.VarCharValue)
		}
		return false
	}
	if a.TimeValue.Unix() != b.TimeValue.Unix() {
		if showDiff {
			fmt.Println("[Diff] TimeValue:", a.TimeValue, b.TimeValue)
		}
		return false
	}
	return true
}

func CompareDataSet(base, current []*DataInstance, showSummary bool) bool {
	originMap := make(map[int64]*DataInstance)
	statMap := make(map[int64]int)
	for _, item := range base {
		originMap[item.PK] = item
		statMap[item.PK] = dataLost
	}
	isSame := true
	for _, item := range current {
		if _, ok := originMap[item.PK]; !ok {
			isSame = false
			continue
		}
		if Compare(item, originMap[item.PK], false) {
			statMap[item.PK] = dataSame
		} else {
			statMap[item.PK] = dataNotSame
			isSame = false
		}
	}
	if showSummary {
		_ = DumpSelectInfo(statMap)
	}
	return isSame
}

func DumpSelectInfo(selectResult map[int64]int) bool {
	sameDataCount := 0
	notSameDataCount := 0
	lostDataCount := 0
	total := len(selectResult)
	for _, item := range selectResult {
		if item == dataSame {
			sameDataCount += 1
		}
		if item == dataNotSame {
			notSameDataCount += 1
		}
		if item == dataLost {
			lostDataCount += 1
		}
	}
	fmt.Println("total: ", total)
	fmt.Println("right: ", sameDataCount)
	fmt.Println("wrong: ", notSameDataCount)
	fmt.Println("lost:  ", lostDataCount)
	return total == sameDataCount
}
