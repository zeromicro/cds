package handle

import (
	json1 "encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/zeromicro/cds/cmd/rtu/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/rtu/model"
)

var mysql1 = []byte(`{"schema":{"type":"struct","fields":[{"type":"struct","fields":
[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,
"field":"first_name"},{"type":"string","optional":false,"field":"last_name"},
{"type":"string","optional":false,"field":"email"}],"optional":true,"name":
"dbserver1.inventory.customers.Value","field":"before"},{"type":"struct","fields":
[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"first_name"},
{"type":"string","optional":false,"field":"last_name"},{"type":"string","optional":false,"field":"email"}],
"optional":true,"name":"dbserver1.inventory.customers.Value","field":"after"},{"type":"struct","fields":
[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},
{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},
{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"}
,"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,
"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},
{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},
{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string",
"optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},
{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct",
"fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},
{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],
"optional":false,"name":"dbserver1.inventory.customers.Envelope"},"payload":{"before":null,"after":{"id":1011,
"first_name":"Kenneth12","last_name":"Anderson12","email":"kander12@acme.com"},"source":{"version":"1.1.1.Final",
"connector":"mysql","name":"dbserver1","ts_ms":1588747759000,"snapshot":"false","db":"inventory","table":"customers",
"server_id":223344,"gtid":null,
"file":"mysql-bin.000003","pos":1305,"row":0,"thread":3,"query":null},"op":"c","ts_ms":1588747759949,"transaction":null}}`)

// var mysql2 = []byte(`{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"string","optional":false,"field":"user_id"}],"optional":true,"name":"dbserver1.inventory.user_tags.Value","field":"before"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"user_id"}],"optional":true,"name":"dbserver1.inventory.user_tags.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.user_tags.Envelope"},"payload":{"before":null,"after":{"user_id":"1211"},"source":{"version":"1.1.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1588844399000,"snapshot":"false","db":"inventory","table":"user_tags","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":4833,"row":0,"thread":3,"query":null},"op":"c","ts_ms":1588844399190,"transaction":null}}`)
var mongo = []byte(`{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"after"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"patch"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"filter"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":false,"field":"rs"},{"type":"string","optional":false,"field":"collection"},{"type":"int32","optional":false,"field":"ord"},{"type":"int64","optional":true,"field":"h"},{"type":"int64","optional":true,"field":"tord"}],"optional":false,"name":"io.debezium.connector.mongo.Source","field":"source"},{"type":"string","optional":true,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"fullfillment1.inventory.inventory.Envelope"},"payload":{"after":null,"patch":"{\"$v\": 1,\"$set\": {\"b\": 3.0}}","filter":"{\"_id\": {\"$oid\": \"5eb626b3452cd570b405bf7d\"}}","source":{"version":"1.1.1.Final","connector":"mongodb","name":"fullfillment1","ts_ms":1588995826000,"snapshot":"false","db":"inventory","rs":"rs","collection":"inventory","ord":1,"h":2083074164920995683,"tord":null},"op":"u","ts_ms":1588995826999,"transaction":null}}`)

func TestGetSchema(t *testing.T) {
	cases := []struct {
		category string
		b        []byte
	}{
		{
			model.DBZUMMYSQL,
			mysql1,
		},
		{
			model.DBZUMMONGO,
			mongo,
		},
	}
	for _, c := range cases {
		p := newParseEngine(c.category, newRunEngine(&config.Job{
			Target: struct {
				Type    string
				Shards  [][]string
				ChProxy string
				Table   string
				Db      string
			}{Table: "1", Db: "2"},
			Source: struct {
				Type     string
				Topic    string
				Dsn      string
				Table    string
				QueryKey string
			}{Type: "canal-mysql"},
		}))
		obj := p.jsonToObj(c.b)
		if obj[0].GetCategory() != c.category {
			t.Fail()
		}
	}
}

func BenchmarkGetSchema(b *testing.B) {
	b.Run("json", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			obj := &model.DebeziumMySQL{}
			_ = json1.Unmarshal(mysql1, obj)
		}
	})
	b.ResetTimer()

	jsonitor := jsoniter.ConfigCompatibleWithStandardLibrary
	b.Run("json-iterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			obj := &model.DebeziumMySQL{}
			_ = jsonitor.Unmarshal(mysql1, obj)
		}
	})
}

func TestMerge(t *testing.T) {
	p := newParseEngine(model.CANALMYSQL, newRunEngine(&config.Job{
		Target: struct {
			Type    string
			Shards  [][]string
			ChProxy string
			Table   string
			Db      string
		}{Table: "1", Db: "2"},
		Source: struct {
			Type     string
			Topic    string
			Dsn      string
			Table    string
			QueryKey string
		}{Type: "canal-mysql"},
	}))
	l := make([]model.DataInterface, 0, 10)
	a := &model.CanalMysql{
		Op: "c",
	}
	a.SetExistsKeys([]int8{1, 1})
	a.SetValues([]interface{}{1, ""})
	l = append(l, a)
	a = &model.CanalMysql{
		Op: "u",
	}
	a.SetExistsKeys([]int8{1, 1})
	a.SetValues([]interface{}{1, "1"})
	l = append(l, a)

	r := p.merge(l, 0)
	if len(r) != 1 {
		t.Fail()
	}
	if r[0].GetValues()[1] != "1" {
		t.Fail()
	}
}
