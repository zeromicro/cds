package dbtool

import (
	"math/rand"
	"time"
)

type DataInstance struct {
	PK           int64     `db:"pk" bson:"pk"`
	IntValue     int       `db:"int_value" bson:"int_value"`
	FloatValue   float32   `db:"float_value" bson:"float_value"`
	DoubleValue  float64   `db:"double_value" bson:"double_value"`
	CharValue    string    `db:"char_value" bson:"char_value"`
	VarCharValue string    `db:"varchar_value" bson:"varchar_value"`
	TimeValue    time.Time `db:"time_value" bson:"time_value"`
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func generateData() *DataInstance {
	pk := time.Now().UnixNano() + int64(rand.Intn(1000))
	intValue := rand.Int()
	floatValue := rand.Float32()
	doubleValue := rand.Float64()
	charValue := randStringRunes(rand.Intn(17))
	varCharValue := randStringRunes(rand.Intn(17))
	now := time.Now()
	timeValue := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.Local)
	return &DataInstance{PK: pk, IntValue: intValue, FloatValue: floatValue, DoubleValue: doubleValue,
		CharValue: charValue, VarCharValue: varCharValue, TimeValue: timeValue}
}

func GenerateDataSet(num int) []*DataInstance {
	keyMap := make(map[int64]bool)
	var result []*DataInstance
	for i := 0; i < num; i++ {
		dataIns := generateData()
		if _, ok := keyMap[dataIns.PK]; ok {
			i--
			continue
		}
		result = append(result, generateData())
	}
	return result
}

func generateDataSetBatch(num, batch int) [][]*DataInstance {
	keyMap := make(map[int64]bool)
	var result [][]*DataInstance
	for i := 0; i < batch; i++ {
		var batchResult []*DataInstance
		for j := 0; j < num; j++ {
			dataIns := generateData()
			if _, ok := keyMap[dataIns.PK]; ok {
				j--
				continue
			}
			batchResult = append(batchResult, generateData())
		}
		result = append(result, batchResult)
	}
	return result
}

func UpdateDataSet(dataSet []*DataInstance) []*DataInstance {
	var result []*DataInstance
	for _, item := range dataSet {
		ins := &DataInstance{PK: item.PK}
		ins.IntValue = rand.Int()
		ins.FloatValue = rand.Float32()
		ins.DoubleValue = rand.Float64()
		ins.CharValue = randStringRunes(rand.Intn(17))
		ins.VarCharValue = randStringRunes(rand.Intn(17))
		now := time.Now()
		ins.TimeValue = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.Local)
		result = append(result, ins)
	}
	return result
}
