package mysqlx

import (
	"errors"
	"strings"

	"github.com/tal-tech/cds/pkg/strx"
	"github.com/tal-tech/go-zero/core/logx"

	table2 "github.com/tal-tech/cds/pkg/table"
)

func ToClickhouseTable(dsn string, db, table, indexes string, withTime bool) ([]string, string, error) {
	columns, e := DescribeMysqlTable(TakeMySQLConnx(dsn), table)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	pri, createTime, updateTime := "", "", ""
	newColumns := make([]table2.Column, 0, len(columns))
	for i, c := range columns {
		if c.Key == "PRI" {
			pri = c.Field
		}
		if c.Field == "create_time" || c.Field == "createTime" {
			createTime = c.Field
		}
		if c.Field == "update_time" || c.Field == "updateTime" || c.Field == "insert_id" {
			updateTime = c.Field
		}
		// type converter
		columns[i].Type = toClickhouseType(c.Type)
		newColumns = append(newColumns, table2.Column{
			Name:    columns[i].Field,
			Type:    columns[i].Type,
			Comment: columns[i].Comment,
		})
	}
	if pri == "" {
		return nil, "", errors.New("未能自动识别" + table + "表的主键")
	}
	if indexes == "" {
		indexes = pri
	}

	data := table2.TableMeta{
		QueryKey:   pri,
		Columns:    newColumns,
		DB:         db,
		Table:      table,
		CreateTime: createTime,
		Indexes:    indexes,
		UpdateTime: updateTime,
		WithTime:   withTime,
	}

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

func toClickhouseType(typ string) string {
	after := strx.SubAfterLast(typ, ")", "")
	typ = strx.SubBeforeLast(typ, "(", typ)
	typ = strings.ToLower(typ)
	switch typ {
	case "bool", "boolean", "tinyint":
		return withUnsigned("Int8", after)
	case "smallint", "year":
		return withUnsigned("Int16", after)
	case "bit", "mediumint", "int", "integer":
		return withUnsigned("Int32", after)
	case "bigint":
		return withUnsigned("Int64", after)
	case "decimal", "dec", "float", "double", "double precision", "float unsigned":
		return "Float64"
	case "date":
		return "Date"
	case "datetime", "timestamp", "time":
		return "DateTime"
	case "enum":
		return "LowCardinality(String)"
	// case "char","varchar","binary","varbinary","blob","text","set","json":
	default:
		return "String"
	}
}

func withUnsigned(typ, after string) string {
	if after == "unsigned" {
		return "U" + typ
	}
	return typ
}
