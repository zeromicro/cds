package model

import (
	"testing"
	"time"
)

const layout = "2006-01-02 15:04:05 -0700 MST"

var (
	createData = []byte(`{"data":[{"id":"8","date":"2020-07-06 14:23:39","count":"19","event":"19"}],"database":"hera_test","es":1594016619000,"id":105,"isDdl":false,"mysqlType":{"id":"int(11)","date":"datetime","count":"int(11)","event":"varchar(4)"},"old":null,"pkNames":["id"],"sql":"","sqlType":{"id":4,"date":93,"count":4,"event":12},"table":"event_data","ts":1594016619978,"type":"INSERT"}`)
	updateData = []byte(`{"data":[{"id":"7","date":"2020-07-06 14:17:13","count":"15","event":"13"}],"database":"hera_test","es":1594016333000,"id":90,"isDdl":false,"mysqlType":{"id":"int(11)","date":"datetime","count":"int(11)","event":"varchar(4)"},"old":[{"count":"13"}],"pkNames":["id"],"sql":"","sqlType":{"id":4,"date":93,"count":4,"event":12},"table":"event_data","ts":1594016333737,"type":"UPDATE"}`)
	deleteData = []byte(`{"data":[{"id":"6","date":"2020-07-06 14:14:11","count":"12","event":"12"}],"database":"hera_test","es":1594016654000,"id":107,"isDdl":false,"mysqlType":{"id":"int(11)","date":"datetime","count":"int(11)","event":"varchar(4)"},"old":null,"pkNames":["id"],"sql":"","sqlType":{"id":4,"date":93,"count":4,"event":12},"table":"event_data","ts":1594016654616,"type":"DELETE"}`)

	tableStruct = &SQLTable{
		Columns:    []string{"id", "date", "count", "event", "ck_is_delete"},
		PrimaryKey: "id",
		Types: map[string]DataType{
			"id":           DataTypeInt,
			"date":         DataTypeTime,
			"count":        DataTypeInt,
			"event":        DataTypeInt,
			"ck_is_delete": DataTypeInt,
		},
	}

	badCase       = []byte(`{"data":[{"id":"4","campus_id":"10026","year":"2019","first_term_start":"2020-08-04 16:59:10","first_term_end":"2020-01-31 23:59:59","second_term_start":"2020-02-01 00:00:00","second_term_end":"2020-06-30 00:00:00","activate_time":null,"create_time":"2020-07-10 17:43:37","update_time":"2020-08-04 16:59:10"}],"database":"dyt_test","es":1596531550000,"id":54,"isDdl":false,"mysqlType":{"id":"bigint(20)","campus_id":"int(10)","year":"int(10)","first_term_start":"timestamp","first_term_end":"timestamp","second_term_start":"timestamp","second_term_end":"timestamp","activate_time":"timestamp","create_time":"timestamp","update_time":"timestamp"},"old":[{"year":"2020","first_term_start":"2020-08-04 16:58:38","update_time":"2020-08-04 16:58:38"}],"pkNames":["id"],"sql":"","sqlType":{"id":-5,"campus_id":4,"year":4,"first_term_start":93,"first_term_end":93,"second_term_start":93,"second_term_end":93,"activate_time":93,"create_time":93,"update_time":93},"table":"term","ts":1596531550810,"type":"UPDATE"}`)
	badCaseStruct = &SQLTable{
		Types: map[string]DataType{
			"id":            DataTypeInt,
			"activate_time": DataTypeTime,
		},
		PrimaryKey: "id",
		Columns:    []string{"id", "activate_time"},
	}
	pool = NewMapPool()
)

func TestCanalCreate(t *testing.T) {
	obj := &CanalMysql{}
	err := obj.UnmarshalFromByte(createData, pool)
	// err := json.Unmarshal(createData, obj)
	if err != nil {
		t.Error(err)
	}
	result, err := obj.ParseToMap(tableStruct)
	if err != nil {
		t.Error(err)
	}
	ti, _ := time.ParseInLocation("2006-01-02 15:04:05", "2020-07-06 14:23:39", time.Local)
	if result["id"] != 8 || result["count"] != 19 || result["event"] != 19 || !result["date"].(*time.Time).Equal(ti) {
		t.Log(result["date"].(*time.Time).UTC().Format(layout))
		t.Fail()
	}
}

func TestCanalUpdate(t *testing.T) {
	obj := &CanalMysql{}
	err := obj.UnmarshalFromByte(updateData, pool)
	if err != nil {
		t.Error(err)
	}
	result, err := obj.ParseToMap(tableStruct)
	if err != nil {
		t.Error(err)
	}
	ti, _ := time.ParseInLocation("2006-01-02 15:04:05", "2020-07-06 14:17:13", time.Local)
	if result["id"] != 7 || result["count"] != 15 || result["event"] != 13 || !result["date"].(*time.Time).Equal(ti) {
		t.Log(result["date"].(*time.Time).UTC().Format(layout))
		t.Fail()
	}
}

func TestCanalDelete(t *testing.T) {
	obj := &CanalMysql{}
	err := obj.UnmarshalFromByte(deleteData, pool)
	if err != nil {
		t.Error(err)
	}
	result, err := obj.ParseToMap(tableStruct)
	if err != nil {
		t.Error(err)
	}
	if result["id"] != 6 {
		t.Fail()
	}
}

func TestCanalBadCase(t *testing.T) {
	obj := &CanalMysql{}
	err := obj.UnmarshalFromByte(badCase, pool)
	if err != nil {
		t.Error(err)
	}
	result, err := obj.ParseToMap(badCaseStruct)
	if err != nil {
		t.Error(err)
	}
	if result["activate_time"].(*time.Time).String() != NullValMap[DataTypeTime].(time.Time).String() || result["id"] != 4 {
		t.Fail()
	}
}

func TestCanalMysql_setValueMap(t *testing.T) {
	obj := &CanalMysql{}
	m := map[string]interface{}{"a": "1.234", "b": 2.4}
	obj.setValueMap(m, &SQLTable{
		Types: map[string]DataType{"a": DataTypeFloat, "b": DataTypeFloat},
	}, false)
	if m["a"] != 1.234 {
		t.Fatal()
	}
	if m["b"] != 2.4 {
		t.Fatal()
	}
}
