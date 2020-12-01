package mysqlx

import (
	"bytes"
	"errors"
	"regexp"
	"strings"
	"text/template"

	"github.com/tal-tech/cds/tools/strx"

	"github.com/tal-tech/go-zero/core/logx"
)

const (
	createEntityTableSql = `CREATE TABLE if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}` + "`" + ` ON CLUSTER bip_ck_cluster
	(
       insert_id UInt64 COMMENT '插入id unix timestamp nano second',
	  {{range .Columns}}
	  ` + "`" + `{{.Field}}` + "`" + ` {{.Type}} COMMENT '{{.Comment}} @{{$.DB}}库{{$.Table}}表.{{.Field}}',{{end}}
		ck_is_delete UInt8 	COMMENT '用于记录状态 0为正常状态 1为删除状态'
	) ENGINE ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_{{.DB}}_{{.Table}}',
			 '{replica}') PARTITION BY toYYYYMM({{.CreateTime}}) ORDER BY({{.Indexes}}) SETTINGS index_granularity = 8192;`

	createAllTableSql = `CREATE TABLE if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_all` + "`" + ` ON CLUSTER bip_ck_cluster
	(
	   insert_id UInt64 COMMENT '插入id unix timestamp nano second',
	  {{range .Columns}}
	  ` + "`" + `{{.Field}}` + "`" + ` {{.Type}} COMMENT '{{.Comment}} @{{$.DB}}库{{$.Table}}表.{{.Field}}',{{end}}
	  ck_is_delete UInt8 	COMMENT '用于记录状态 0为正常状态 1为删除状态'
	) ENGINE Distributed(bip_ck_cluster, '{{.DB}}', '{{.Table}}', sipHash64({{.QueryKey}}));`

	// 单独查询节点sql
	createQueryNodeSql = `CREATE TABLE if not exists ` + "`" + `{{.DB}}` + "`" + `.` + "`" + `{{.Table}}_all` + "`" + `
	(
	   insert_id UInt64 COMMENT '插入id unix timestamp nano second',
	  {{range .Columns}}
	  ` + "`" + `{{.Field}}` + "`" + ` {{.Type}} COMMENT '{{.Comment}} @{{$.DB}}库{{$.Table}}表.{{.Field}}',{{end}}
	  ck_is_delete UInt8 	COMMENT '用于记录状态 0为正常状态 1为删除状态'
	) ENGINE Distributed(bip_ck_cluster, '{{.DB}}', '{{.Table}}', sipHash64({{.QueryKey}}));`
)

var re = regexp.MustCompile(`(?m)^\s*$[\r\n]*|[\r\n]+\s+\z`)
var templates = []string{
	createEntityTableSql,
	createAllTableSql,
	createQueryNodeSql,
}

func ToClickhouseTable(dsn string, db, table, indexes string) ([]string, string, error) {
	columns, e := DescribeMysqlTable(TakeMySQLConnx(dsn), table)
	if e != nil {
		logx.Error(e)
		return nil, "", e
	}
	pri, createTime, updateTime := "", "", ""
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
	}
	if pri == "" {
		return nil, "", errors.New("未能自动识别" + table + "表的主键")
	}
	if indexes == "" {
		indexes = pri
	}

	out := []string{}
	data := struct {
		QueryKey   string
		Columns    []*Column
		DB         string
		Table      string
		CreateTime string
		UpdateTime string
		Indexes    string
	}{
		QueryKey:   pri,
		Columns:    columns,
		DB:         db,
		Table:      table,
		CreateTime: createTime,
		Indexes:    indexes,
		UpdateTime: updateTime,
	}

	for _, templateSql := range templates {
		buf := bytes.NewBufferString("")
		t, e := template.New("name").Parse(templateSql)
		if e != nil {
			logx.Error(e)
			return nil, "", e
		}
		e = t.Execute(buf, data)
		if e != nil {
			logx.Error(e)
			return nil, "", e
		}

		sql := re.ReplaceAllString(buf.String(), "")
		out = append(out, sql)
	}

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
