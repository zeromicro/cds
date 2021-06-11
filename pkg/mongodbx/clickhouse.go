package mongodbx

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

	table2 "github.com/tal-tech/cds/pkg/table"
)

func ToClickhouseTable(dsn string, db, tablename, indexes string, withTime bool) ([]string, string, error) {
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
	c, e := cli.Database(info.Database).Collection(tablename).EstimatedDocumentCount(context.TODO())
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	if c == 0 {
		return nil, "", errors.New("没有数据，无法生成建表语句")
	}
	data := &table2.TableMeta{
		DB:       db,
		Table:    tablename,
		Indexes:  indexes,
		M:        make(map[string]int),
		WithTime: withTime,
	}

	err := getColumns(cli, info.Database, tablename, data, true)
	if err != nil {
		return nil, "", err
	}
	err = getColumns(cli, info.Database, tablename, data, false)
	if err != nil {
		return nil, "", err
	}
	for i := range data.Columns {
		if data.Columns[i].Type == "" {
			data.Columns[i].Type = "String"
		}
	}

	if data.QueryKey == "" {
		return nil, "", errors.New("未能自动识别主键_id")
	}
	if data.Indexes == "" {
		data.Indexes = data.QueryKey
	}

	sort.Slice(data.Columns, func(i, j int) bool {
		return data.Columns[i].Name <= data.Columns[j].Name
	})
	out := make([]string, 0, 8)

	// megreTree table
	out = append(out, data.CreateTable(table2.MTLocal, true))
	// distrubuted table for query node
	out = append(out, data.CreateTable(table2.Distribute, false))
	// distributed table for data node
	out = append(out, data.CreateTable(table2.Distribute, true))

	// mv inner table
	out = append(out, data.CreateTable(table2.MvInner, true))

	//
	out = append(out, data.CreateTable(table2.MvLocal, true))
	out = append(out, data.CreateTable(table2.MvDistribute, true))
	out = append(out, data.CreateTable(table2.MvDistribute, false))
	out = append(out, data.CreateTable(table2.MvNow, false))
	out = append(out, data.CreateTable(table2.MvNow, true))

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

func getMongoDataCursor(cli *mongo.Client, db, table string, reverse bool) (*mongo.Cursor, error) {
	// order 1: 正序, -1: 倒叙
	order := 1
	if !reverse {
		order = -1
	}
	opts := options.Find()
	opts.SetSort(bson.D{{Key: "$natural", Value: order}})
	opts.SetLimit(1000)

	cur, err := cli.Database(db).Collection(table).Find(context.TODO(), bson.M{}, opts)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	return cur, nil
}

func getColumnNameAndTypeFromBsonM(v *bson.M, data *table2.TableMeta) error {
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
		// type converter
		column := table2.Column{
			Name: k,
		}
		if v == nil {
			if k == "updateTime" {
				column.Type = "DateTime"
			} else {
				column.Type = ""
			}
		} else {
			column.Type = toClickhouseType(reflect.TypeOf(v))
		}
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
		if v, ok := data.M[column.Name]; ok {
			if data.Columns[v].Type == "" && column.Type != "" {
				data.Columns[v].Type = column.Type
			}
		} else {
			data.M[column.Name] = len(data.Columns)
			data.Columns = append(data.Columns, column)
		}
	}
	return nil
}

func getColumns(cli *mongo.Client, db, table string, data *table2.TableMeta, reverse bool) error {
	cur, e := getMongoDataCursor(cli, db, table, reverse)
	if e != nil {
		logx.Error(e)
		return e
	}
	for cur.Next(context.TODO()) {
		v := new(bson.M)

		err := cur.Decode(v)
		if err != nil {
			logx.Error(err)
			continue
		}
		err = getColumnNameAndTypeFromBsonM(v, data)
		if err != nil {
			logx.Error(err)
			return err
		}
	}
	return nil
}
