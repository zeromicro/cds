package mysqlx

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type count struct {
	count int64 `db:"count"`
}

func GenerateSQLInsert(table string, data interface{}) string {
	sql := "insert into " + table + " values ("
	dataType := reflect.TypeOf(data)
	for i := 0; i < dataType.NumField(); i++ {
		sql += "?,"
	}
	sql = strings.TrimSuffix(sql, ",")
	return sql + ")"
}

// CountSQL count the target sql
func CountSQL(sql string) string {
	prefix := `SELECT count() FROM (`
	suffix := ")"
	sql = prefix + sql + suffix
	return sql
}

// PageSQL add page limit to sql, return pagedSQL, offset, limit
func PageSQL(sql string, page, pageSize int) (string, int, int) {
	sql += " LIMIT ?,?"
	return sql, page * pageSize, pageSize
}

func MysqlListTable(db *sql.DB) ([]string, error) {
	rows, e := db.Query(`show tables`)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	columns, e := rows.Columns()
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	if len(columns) < 1 {
		return nil, errors.New("查询结果的列数量为0")
	}

	out := []string{}
	for rows.Next() {
		name := sql.RawBytes{}
		e := rows.Scan(&name)
		if e != nil {
			logx.Error(e)
			return nil, e
		}

		out = append(out, string(name))
	}
	return out, nil
}

func MysqlPing(dsn string) error {
	db, e := sql.Open("mysql", dsn)
	if e != nil {
		return e
	}
	defer db.Close()

	return db.Ping()
}

func DescribeMysqlTable(conn sqlx.SqlConn, table string) ([]*Column, error) {
	vs := []*Column{}
	e := conn.QueryRows(&vs, "show full columns  from  `"+table+"`")
	if e != nil {
		return nil, e
	}
	return vs, nil
}

// ParseMySQLDatabase 会在连接串中加上时间类型和时区设置，返回修改后的dsn、数据库名称和error
func ParseMySQLDatabase(dsn string) (string, string, error) {
	info, e := mysql.ParseDSN(dsn)
	if e != nil {
		return "", "", e
	}
	db := info.DBName
	if db == "" {
		return "", "", errors.New("连接串没写database")
	}
	info.Loc = time.Local
	info.ParseTime = true
	return info.FormatDSN(), db, nil
}

func CreateDbIne(dsn string) error {
	info, e := mysql.ParseDSN(dsn)
	if e != nil {
		return e
	}
	db := info.DBName
	if db == "" {
		return errors.New("连接串没写database")
	}
	info.Loc = time.Local
	info.ParseTime = true
	info.DBName = "mysql"
	url := info.FormatDSN()
	sql := `create database if not exists ` + db
	conn := sqlx.NewMysql(url)
	_, e = conn.Exec(sql)
	return e
}

func CreateMysqlIndexes(conn sqlx.SqlConn, db, table string, indexes []string) error {
	for _, index := range indexes {
		_, e := conn.Exec("create index " + index + "_idx on " + db + "." + table + " (`" + index + "`)")
		if e != nil {
			return e
		}
	}
	return nil
}

type MysqlTableComment struct {
	TableComment string `db:"TABLE_COMMENT"`
}

func GetMysqlTableComment(source, table string) (string, error) {
	query := `select TABLE_COMMENT from information_schema.TABLES where TABLE_SCHEMA=? and TABLE_NAME=?`
	dsn, db, e := ParseMySQLDatabase(source)
	if e != nil {
		return "", e
	}
	conn := sqlx.NewMysql(dsn)
	v := MysqlTableComment{}
	e = conn.QueryRow(&v, query, db, table)
	if e != nil {
		return "", e
	}
	return v.TableComment, nil
}

func CountTable(c sqlx.SqlConn, table string) (int64, error) {
	v := count{}
	e := c.QueryRow(&v, `select count(*) as count from `+table)
	if e != nil {
		return 0, e
	}
	return v.count, nil
}
