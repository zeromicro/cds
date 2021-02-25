package model

import (
	"testing"
)

var (
	insert    = "{\"_id\": {\"_data\": {\"$binary\": \"gl7w75QAAAABRh5faWQAKwIAWhAEqeMNaUKiTnq73NLCoKYrnAQ=\", \"$type\": \"00\"}, \"_typeBits\": {\"$binary\": \"AQ==\", \"$type\": \"00\"}}, \"operationType\": \"insert\", \"fullDocument\": {\"_id\": 1.0}, \"ns\": {\"db\": \"aaa\", \"coll\": \"bbb\"}, \"documentKey\": {\"_id\": 1.0}}"
	updateStr = "{\"_id\": {\"_data\": {\"$binary\": \"gl7w8DUAAAABRh5faWQAKwIAWhAEqeMNaUKiTnq73NLCoKYrnAQ=\", \"$type\": \"00\"}, \"_typeBits\": {\"$binary\": \"AQ==\", \"$type\": \"00\"}}, \"operationType\": \"replace\", \"fullDocument\": {\"_id\": 1.0, \"a\": [1.0, 2.0, 3.0]}, \"ns\": {\"db\": \"aaa\", \"coll\": \"bbb\"}, \"documentKey\": {\"_id\": 1.0}}"
	badCase1  = "{\"_id\": {\"_id\": {\"$oid\": \"5d2861007cb5fd0001701fd5\"}, \"copyingData\": true}, \"operationType\": \"insert\", \"ns\": {\"db\": \"punchcard\", \"coll\": \"Result\"}, \"documentKey\": {\"_id\": {\"$oid\": \"5d2861007cb5fd0001701fd5\"}}, \"fullDocument\": {\"_id\": {\"$oid\": \"5d2861007cb5fd0001701fd5\"}, \"content\": \"he\", \"taskId\": \"5cc6a35bcfeb630001a322cc\", \"cardPunchId\": \"5cc6a35bcfeb630001a32281\", \"senderId\": \"5be13908e368b909de1f2b13\", \"classroomId\": \"5cc69b59a4d0210001ea0a29\", \"createTime\": {\"$date\": 1562927360503}, \"updateTime\": {\"$date\": 1562927360503}, \"tag\": \"读书\"}}"
)

func TestConnectorMongo_Insert(t *testing.T) {
	obj := &ConnectorMongo{}
	err := json.UnmarshalFromString(insert, obj)
	if err != nil {
		t.Error(err)
	}
	t.Logf("%v", obj)
}

func TestConnectorMongo_Update(t *testing.T) {
	obj := &ConnectorMongo{}
	err := json.UnmarshalFromString(updateStr, obj)
	if err != nil {
		t.Error(err)
	}
	table := &SQLTable{
		Types: map[string]DataType{
			"_id": DataTypeInt,
			"a":   DataTypeString,
		},
		PrimaryKey: "_id",
		Columns:    []string{"_id", "a"},
	}
	v, err := obj.ParseToMap(table)
	if err != nil {
		t.Error(err)
	}
	t.Logf("%v", v)
}

func TestBadCase(t *testing.T) {
	obj := &ConnectorMongo{}
	err := json.Unmarshal([]byte(badCase1), obj)
	if err != nil {
		t.Error(err)
	}
	t.Log(obj)
}

func TestUpdate(t *testing.T) {
	pool = NewMapPool()

	b := []byte(`"{\"_id\": {\"_data\": \"825F3DEA50000000012B022C0100296E5A1004BE90A52A002646F8AFF408AD4A9FD28946645F696400645F3C9208CFD75E00010060770004\"}, \"operationType\": \"update\", \"clusterTime\": {\"$timestamp\": {\"t\": 1597893200, \"i\": 1}}, \"ns\": {\"db\": \"homework\", \"coll\": \"homework\"}, \"documentKey\": {\"_id\": {\"$oid\": \"5f3c9208cfd75e0001006077\"}}, \"updateDescription\": {\"UpdatedFields\": {\"resultMarkType\": \"Star\"}, \"RemovedFields\": [\"bb\"]}}"`)
	obj := &ConnectorMongo{}
	err := obj.UnmarshalFromByte(b, pool)
	if err != nil {
		t.Fatal(err)
	}
	table := &SQLTable{
		Types: map[string]DataType{
			"_id":            DataTypeString,
			"resultMarkType": DataTypeString,
			"bb":             DataTypeString,
		},
		PrimaryKey: "_id",
		Columns:    []string{"_id", "bb", "resultMarkType"},
	}
	v, err := obj.ParseToMap(table)
	if err != nil {
		t.Error(err)
	}
	t.Log(v)
}

func TestDatetime(t *testing.T) {
	pool = NewMapPool()
	b := ``
	obj := &ConnectorMongo{}
	err := obj.UnmarshalFromStr(b, pool)
	if err != nil {
		t.Fatal(err)
	}
	table := &SQLTable{
		Types: map[string]DataType{
			"_id":        DataTypeString,
			"createTime": DataTypeTime,
		},
		PrimaryKey: "_id",
		Columns:    []string{"_id", "createTime"},
	}
	v, err := obj.ParseToMap(table)
	if err != nil {
		t.Error(err)
	}
	t.Log(v)
}
