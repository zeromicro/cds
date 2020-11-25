package mongodbx

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"text/template"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

type Column struct {
	Name string
	Type string
}

func ToClickhouseTable(dsn string, db, table, indexes string) ([]string, string, error) {
	info, e := connstring.Parse(dsn)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	cli, e := TakeMongoClient(dsn)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	c, e := cli.Database(info.Database).Collection(table).EstimatedDocumentCount(context.TODO())
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	if c == 0 {
		return nil, "", errors.New("没有数据，无法生成建表语句")
	}
	r := make([]*bson.M, 0, 2000)

	// 倒序取 1000
	opts := options.Find()
	opts.SetSort(bson.D{{"$natural", -1}})
	opts.SetLimit(1000)

	cur, e := cli.Database(info.Database).Collection(table).Find(context.TODO(), bson.M{}, opts)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	for cur.Next(context.TODO()) {
		v := new(bson.M)

		err := cur.Decode(v)
		if err != nil {
			logx.Error(err)
			continue
		}
		r = append(r, v)
	}

	// 正序取 1000
	opts = options.Find()
	opts.SetSort(bson.D{{"$natural", 1}})
	opts.SetLimit(1000)

	cur, e = cli.Database(info.Database).Collection(table).Find(context.TODO(), bson.M{}, opts)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	for cur.Next(context.TODO()) {
		v := new(bson.M)

		err := cur.Decode(v)
		if err != nil {
			logx.Error(err)
			continue
		}
		r = append(r, v)
	}

	data := struct {
		QueryKey   string
		Columns    []Column
		DB         string
		Table      string
		CreateTime string
		UpdateTime string
		Indexes    string
		m          map[string]int
	}{
		DB:      db,
		Table:   table,
		Indexes: indexes,
		m:       make(map[string]int),
	}
	for _, v := range r {
		for k, v := range *v {
			if k == "_id" {
				data.QueryKey = "_id"
			}
			if k == "createTime" || k == "create_time" {
				data.CreateTime = k
			}
			if k == "updateTime" || k == "update_time" || k == "insert_id" {
				data.UpdateTime = k
			}
			//type converter
			column := Column{
				Name: k,
			}
			column.Type = toClickhouseType(reflect.TypeOf(v))
			if val, ok := v.(string); ok && column.Type == "String" && (len(val) == 19 || len(val) == 23) {
				_, err := time.Parse("2006-01-02 15:04:05.000", val)
				if err == nil {
					column.Type = "DateTime"
					goto TYPEOK
				}
				_, err = time.Parse("2006-01-02 15:04:05", val)
				if err == nil {
					column.Type = "DateTime"
					goto TYPEOK
				}
			}
		TYPEOK:
			if _, ok := data.m[column.Name]; !ok {
				data.m[column.Name] = len(data.Columns)
				data.Columns = append(data.Columns, column)
			}
		}
		if data.QueryKey == "" {
			return nil, "", errors.New("未能自动识别主键_id")
		}
		if data.Indexes == "" {
			data.Indexes = data.QueryKey
		}
	}

	out := []string{}
	//solid
	buf := bytes.NewBufferString("")
	t, e := template.New("name").Parse(`CREATE TABLE if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}` + "`" + ` ON CLUSTER bip_ck_cluster
(
insert_id UInt64 COMMENT '插入id unix timestamp nano second',
{{range .Columns}}
` + "`" + `{{.Name}}` + "`" + ` {{.Type}} ,{{end}}
ck_is_delete UInt8 	COMMENT '用于记录删除状态 0为正常状态 1为删除状态'
) ENGINE ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_{{.DB}}_{{.Table}}',
			 '{replica}') PARTITION BY toYYYYMM({{.CreateTime}}) ORDER BY({{.Indexes}}) SETTINGS index_granularity = 8192;`)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	e = t.Execute(buf, data)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	out = append(out, buf.String())

	//all
	buf = bytes.NewBufferString("")
	t, e = template.New("name").Parse(`
CREATE TABLE if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_all` + "`" + `
(
  insert_id        UInt64 COMMENT '插入id unix timestamp nano second',
  {{range .Columns}}
  ` + "`" + `{{.Name}}` + "`" + ` {{.Type}},{{end}}
  ck_is_delete UInt8 	COMMENT '用于记录删除状态 0为正常状态 1为删除状态'
) ENGINE Distributed(bip_ck_cluster, '{{.DB}}', '{{.Table}}', sipHash64({{.QueryKey}}));`)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	e = t.Execute(buf, data)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	out = append(out, buf.String())

	//all
	buf = bytes.NewBufferString("")
	t, e = template.New("name").Parse(`
	CREATE TABLE if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_all` + "`" + ` ON CLUSTER bip_ck_cluster
	(
	  insert_id        UInt64 COMMENT '插入id unix timestamp nano second',
	  {{range .Columns}}
	  ` + "`" + `{{.Name}}` + "`" + ` {{.Type}},{{end}}
	  ck_is_delete UInt8 	COMMENT '用于记录删除状态 0为正常状态 1为删除状态'
	) ENGINE Distributed(bip_ck_cluster, '{{.DB}}', '{{.Table}}', sipHash64({{.QueryKey}}));`)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	e = t.Execute(buf, data)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	out = append(out, buf.String())

	////now
	//buf = bytes.NewBufferString("")
	//t, e = template.New("name").Parse(`create view if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_now` + "`" + ` as select * from ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_all` + "`" + ` final where ck_is_delete=0`)
	//if e != nil {
	//	logx.Error(e)
	//	return nil, "", e
	//}
	//e = t.Execute(buf, data)
	//if e != nil {
	//	logx.Error(e)
	//	return nil, "", e
	//}
	//out = append(out, buf.String())
	//
	////now
	//buf = bytes.NewBufferString("")
	//t, e = template.New("name").Parse(`create view if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_now` + "`" + ` on cluster bip_ck_cluster as select * from ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_all` + "`" + ` final where ck_is_delete=0`)
	//if e != nil {
	//	logx.Error(e)
	//	return nil, "", e
	//}
	//e = t.Execute(buf, data)
	//if e != nil {
	//	logx.Error(e)
	//	return nil, "", e
	//}
	//out = append(out, buf.String())

	return out, data.QueryKey, nil
}

func toClickhouseType(t reflect.Type) string {
	if t.String() == "primitive.DateTime" {
		return "DateTime"
	}

	switch t.Kind() {
	case reflect.Uint8:
		return "UInt8"
	case reflect.Uint16:
		return "UInt16"
	case reflect.Uint32:
		return "UInt32"
	case reflect.Uint, reflect.Uint64:
		return "UInt64"
	case reflect.Int8:
		return "Int8"
	case reflect.Int16:
		return "Int16"
	case reflect.Int32:
		return "Int32"
	case reflect.Int, reflect.Int64:
		return "Int64"
	case reflect.Bool:
		return "LowCardinality(String)"
	case reflect.Float32:
		return "Float32"
	case reflect.Float64:
		return "Float64"
	default:
		return "String"
	}
}
