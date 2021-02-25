package clickhousex

import (
	"bytes"
	"errors"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/tal-tech/cds/pkg/strx"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/stores/clickhouse"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

type (
	Name struct {
		Name string `db:"name"`
	}
	CreateTableQuery struct {
		CreateTableQuery string `db:"create_table_query"`
	}
)

func GetLengthTag(field reflect.StructField) (int, error) {
	length, ok := field.Tag.Lookup("length")
	if !ok {
		return 0, nil
	}
	return strconv.Atoi(length)
}

func CreateDbClusterIne(dsn string, cluster string) error {
	url, e := url.Parse(dsn)
	if e != nil {
		return e
	}
	query := url.Query()
	db := query.Get("database")
	if db == "" {
		return errors.New("dsn中的database不能为空")
	}
	query.Set("database", "default")
	url.RawQuery = query.Encode()
	conn := clickhouse.New(url.String())

	sql := `create database if not exists ` + db + ` on cluster ` + cluster
	_, e = conn.Exec(sql)
	return e
}

func TruncateTable(c sqlx.SqlConn, table string) error {
	_, e := c.Exec(`truncate table ` + table)
	return e
}

func CreateDbIne(dsn string) error {
	url, e := url.Parse(dsn)
	if e != nil {
		return e
	}
	query := url.Query()
	db := query.Get("database")
	if db == "" {
		return errors.New("dsn中的database不能为空")
	}
	query.Set("database", "default")
	url.RawQuery = query.Encode()
	conn := clickhouse.New(url.String())

	sql := `create database if not exists ` + db
	_, e = conn.Exec(sql)
	return e
}

func ShowTables(c sqlx.SqlConn) ([]string, error) {
	vs := []*Name{}
	query := `show tables`
	e := c.QueryRows(&vs, query)
	if e != nil {
		return nil, e
	}
	out := []string{}
	for _, v := range vs {
		out = append(out, v.Name)
	}
	return out, nil
}

type name struct {
	Name string `db:"name"`
}

func ShowDatabases(c sqlx.SqlConn) ([]string, error) {
	vs := []name{}
	e := c.QueryRows(&vs, `show databases`)
	if e != nil {
		return nil, e
	}
	out := []string{}
	for _, v := range vs {
		out = append(out, v.Name)
	}
	return out, nil
}

func TableExists(c sqlx.SqlConn, table string) (bool, error) {
	_, e := DescribeTable(c, table)
	if e != nil {
		if strings.Contains(e.Error(), "Table") && strings.Contains(e.Error(), "doesn't exist") {
			return false, nil
		}
		return false, e
	}
	return true, nil
}

func DropTable(c sqlx.SqlConn, table string) error {
	_, e := c.Exec(`drop table ` + table)
	return e
}

func DropTableIe(c sqlx.SqlConn, table string) error {
	_, e := c.Exec(`drop table if exists ` + table)
	return e
}

func DropTableClusterIe(c sqlx.SqlConn, cluster, db, table string) error {
	_, e := c.Exec(`drop table if exists ` + db + "." + table + " on cluster " + cluster)
	return e
}

func RenameTable(c sqlx.SqlConn, table, to string) error {
	query := "rename table " + table + " to " + to
	_, e := c.Exec(query)
	return e
}

func GetCreateTableQuery(c sqlx.SqlConn, db, table string) (string, error) {
	v := CreateTableQuery{}
	e := c.QueryRow(&v, `select create_table_query from system.tables where name =? and database=?`, table, db)
	if e != nil {
		return "", e
	}
	return v.CreateTableQuery, nil
}

func CreateTempTable(shards [][]string, db, table string) (string, error) {
	temp := table + "_temp"
	add := 1
	sqlCreateTmp := ""
	var sqlCreate string
	var e2 error
	e := ExecClusterAnyShard(shards, 0, func(dsn string) error {
		sqlCreate, e2 = GetCreateTableQuery(TakeClientx(dsn), db, table)
		return e2
	})
	if e != nil {
		logx.Error(e)
		return "", e
	}
	e = ExecClusterEachShardsAll(shards, 0, func(dsn string) error {
		c := TakeClientx(dsn)
		return DropTableIe(c, temp)
	})
	if e != nil {
		logx.Error(e)
		return "", e
	}

start:
	for _, shard := range shards {
		fail := []error{}
		for _, dsn := range shard {
			c := TakeClientx(dsn)
			for {
				sqlCreateTmp = generateTempSql(sqlCreate, db, table, add)
				_, e = c.Exec(sqlCreateTmp)
				if e != nil {
					if strings.Contains(e.Error(), "already exists") && strings.Contains(e.Error(), "Replica") {
						add++
						goto start
					}
					fail = append(fail, e)
				}
				break
			}
		}
		if len(fail) > 0 && len(fail) == len(shard) {
			return "", fail[0]
		}
	}
	logx.Info(sqlCreateTmp)
	return temp, nil
}

func generateTempSql(sql, db, table string, add int) string {
	sep := "'/clickhouse/tables/{layer}-{shard}/"
	start := strx.SubBefore(sql, sep, sql)
	end := strx.SubAfter(sql, sep, sql)
	zkname := strx.SubBefore(end, "'", "")
	end = strx.SubAfter(end, zkname, end)

	buf := new(bytes.Buffer)
	buf.WriteString(strx.SubBefore(start, db+"."+table, ""))
	buf.WriteString(db + "." + table + "_temp")
	buf.WriteString("(" + strx.SubAfter(start, "(", ""))
	buf.WriteString(sep)
	buf.WriteString(strx.DuplicateName(zkname, add))
	buf.WriteString(end)
	return buf.String()
}
