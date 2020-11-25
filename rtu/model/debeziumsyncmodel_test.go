//+build integration

package model

import (
	"fmt"
	"testing"
)

// mongo
var (
	create  = []byte(`{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"after"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"patch"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"filter"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":false,"field":"rs"},{"type":"string","optional":false,"field":"collection"},{"type":"int32","optional":false,"field":"ord"},{"type":"int64","optional":true,"field":"h"},{"type":"int64","optional":true,"field":"tord"}],"optional":false,"name":"io.debezium.connector.mongo.Source","field":"source"},{"type":"string","optional":true,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"fullfillment1.inventory.inventory.Envelope"},"payload":{"after":"{\"_id\": {\"$oid\": \"5eb626b3452cd570b405bf7d\"},\"a\": 1.0,\"create\": 2.0}","patch":null,"filter":null,"source":{"version":"1.1.1.Final","connector":"mongodb","name":"fullfillment1","ts_ms":1588995763000,"snapshot":"false","db":"inventory","rs":"rs","collection":"inventory","ord":1,"h":171151118119704549,"tord":null},"op":"c","ts_ms":1588995763118,"transaction":null}}`)
	update  = []byte(`{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"after"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"patch"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"filter"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":false,"field":"rs"},{"type":"string","optional":false,"field":"collection"},{"type":"int32","optional":false,"field":"ord"},{"type":"int64","optional":true,"field":"h"},{"type":"int64","optional":true,"field":"tord"}],"optional":false,"name":"io.debezium.connector.mongo.Source","field":"source"},{"type":"string","optional":true,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"fullfillment1.inventory.inventory.Envelope"},"payload":{"after":null,"patch":"{\"$v\": 1,\"$set\": {\"b\": 45.0,\"c\": 2444.0, \"thedate\":{\"$date\": 1533868558652}}}","filter":"{\"_id\": {\"$oid\": \"5eb626b3452cd570b405bf7d\"}}","source":{"version":"1.1.1.Final","connector":"mongodb","name":"fullfillment1","ts_ms":1589005437000,"snapshot":"false","db":"inventory","rs":"rs","collection":"inventory","ord":1,"h":-1386499638266605908,"tord":null},"op":"u","ts_ms":1589005437333,"transaction":null}}`)
	delete1 = []byte(`{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"after"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"patch"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"filter"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":false,"field":"rs"},{"type":"string","optional":false,"field":"collection"},{"type":"int32","optional":false,"field":"ord"},{"type":"int64","optional":true,"field":"h"},{"type":"int64","optional":true,"field":"tord"}],"optional":false,"name":"io.debezium.connector.mongo.Source","field":"source"},{"type":"string","optional":true,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"fullfillment1.inventory.inventory.Envelope"},"payload":{"after":null,"patch":null,"filter":"{\"_id\": {\"$oid\": \"5eb626b3452cd570b405bf7d\"}}","source":{"version":"1.1.1.Final","connector":"mongodb","name":"fullfillment1","ts_ms":1589014173000,"snapshot":"false","db":"inventory","rs":"rs","collection":"inventory","ord":1,"h":-4559077133264410895,"tord":null},"op":"d","ts_ms":1589014173230,"transaction":null}}`)

	table = &SQLTable{
		Columns:    []string{"oid", "a", "create", "thedate"},
		PrimaryKey: "oid",
		Types: map[string]DataType{
			"oid":     DataTypeString,
			"a":       DataTypeFloat,
			"create":  DataTypeInt,
			"thedate": DataTypeTime,
		},
	}
)

func TestCreate(t *testing.T) {
	obj := &DebeziumMongo{}
	err := json.Unmarshal(create, obj)
	if err != nil {
		t.Error(err)
	}
	result, err := obj.ParseToMap(table)
	if err != nil {
		t.Error(err)
	}
	if result["_id"] != "5eb626b3452cd570b405bf7d" || result["a"] != 1.0 || result["create"] != 2 {
		t.Fail()
	}
}

func TestUpdate(t *testing.T) {
	obj := &DebeziumMongo{}
	err := json.Unmarshal(update, obj)
	if err != nil {
		t.Error(err)
	}
	result, err := obj.ParseToMap(table)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(result["thedate"])
	if result["_id"] != "5eb626b3452cd570b405bf7d" || result["b"] != 45.0 || result["c"] != 2444.0 || fmt.Sprint(result["thedate"]) != "2018-08-10 10:35:58.000000652 +0800 CST" {
		t.Fail()
	}
}

func TestDelete(t *testing.T) {
	obj := &DebeziumMongo{}
	err := json.Unmarshal(delete1, obj)
	if err != nil {
		t.Error(err)
	}
	result, err := obj.ParseToMap(table)
	if err != nil {
		t.Error(err)
	}
	if result["_id"] != "5eb626b3452cd570b405bf7d" || len(result) != 1 {
		t.Fail()
	}
}

//func TestLoop(t *testing.T) {
//	clientOptions := options.Client().ApplyURI("mongodb://192.168.56.104:27017")
//
//	// Connect to MongoDB
//	client, err := mongo.Connect(context.TODO(), clientOptions)
//
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Check the connection
//	err = client.Ping(context.TODO(), nil)
//
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	sess := client.Database("aaa").Collection("aaa")
//	sess.InsertOne(context.TODO(), struct {
//		ID int     `json:"id"`
//		Bb float64 `json:"bb"`
//	}{123, 1.2})
//}
