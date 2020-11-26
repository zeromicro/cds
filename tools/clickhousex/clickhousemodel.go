package clickhousex

import (
	"errors"
	"github.com/tal-tech/cds/tools/numx"
	"github.com/tal-tech/cds/tools/strx"
	"log"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/stores/clickhouse"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

type ClickHouseClusterModel struct {
	Conn      sqlx.SqlConn
	AppName   string
	Dsn       string
	Shards    [][]string
	Database  string
	Cluster   string
	TableName string
	Type      reflect.Type
	Indexes   []string

	InsertIDKey string
	FlagKey     string
	FieldDBs    []string
	FieldNames  []string

	sqlCreateTable    string
	sqlAllCreateTable string
	sqlNowCreateView  string
	sqlInsert         string
}

/* NewClickHouseClusterModel 按照data结构体新建model，如果表不存在，则自动创建
 */
func NewClickHouseClusterModel(appName, dsn, cluster string, data interface{}, shards [][]string) (*ClickHouseClusterModel, error) {
	model := &ClickHouseClusterModel{
		AppName: appName,
		Dsn:     dsn,
		Cluster: cluster,
		Shards:  shards,
	}
	dsnInfo, e := url.Parse(dsn)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	model.Database = dsnInfo.Query().Get("database")

	//database
	e = CreateDbClusterIne(dsn, cluster)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = CreateDbIne(dsn)
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	// init data
	model.Conn = clickhouse.New(dsn)
	e = model.initData(data, false)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = model.createTableIfNotExists()
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	e = model.checkColumns()
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return model, nil
}

/* MustNewClickHouseClusterModel 按照data结构体新建model，如果表存在，则删掉重新创建
 */
func MustNewClickHouseClusterModel(appName, dsn, cluster string, data interface{}, shards [][]string) (*ClickHouseClusterModel, error) {
	model := &ClickHouseClusterModel{
		AppName: appName,
		Dsn:     dsn,
		Cluster: cluster,
		Shards:  shards,
	}
	dsnInfo, e := url.Parse(dsn)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	model.Database = dsnInfo.Query().Get("database")

	//database
	e = CreateDbClusterIne(dsn, cluster)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = CreateDbIne(dsn)
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	// init data
	model.Conn = clickhouse.New(dsn)
	e = model.initData(data, true)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = model.DropTableIfExists()
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	e = model.createTableIfNotExists()
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	e = model.checkColumns()
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	return model, nil
}

func (c *ClickHouseClusterModel) initData(data interface{}, must bool) error {
	if c.Database == "" {
		return errors.New("连接串里面没有写database")
	}
	if c.Cluster == "" {
		return errors.New("cluster 不能为空")
	}

	c.Type = reflect.TypeOf(data)
	if c.Type.Kind() == reflect.Ptr {
		c.Type = c.Type.Elem()
	}
	createTimeField, ok := c.Type.FieldByName("CreateTime")
	if !ok {
		return errors.New("CreateTime字段没有设置")
	}
	createTimeDB, ok := createTimeField.Tag.Lookup("db")
	if !ok {
		return errors.New("CreateTime字段没有写db标签")
	}

	if c.AppName != "" {
		c.TableName = strx.ToSnakeCase(c.AppName) + "_" + strx.ToSnakeCase(c.Type.Name())
	} else {
		c.TableName = strx.ToSnakeCase(c.Type.Name())
	}

	ine := "if not exists "
	if must {
		ine = " "
	}
	c.sqlInsert = `insert into ` + c.Database + "." + c.TableName + "_all values("
	c.sqlCreateTable = "create table " + ine + c.Database + "." + c.TableName + " on cluster " + c.Cluster + " ("
	c.sqlAllCreateTable = "create table " + ine + c.Database + "." + c.TableName + "_all on cluster " + c.Cluster + " ("
	for i := 0; i < c.Type.NumField(); i++ {
		field := c.Type.Field(i)
		// tag check
		db, ok := field.Tag.Lookup("db")
		if !ok {
			return errors.New(c.Type.Name() + "类型的" + field.Name + "字段没有写'db' Tag")
		}
		if i == 0 {
			if db != "insert_id" || field.Type.Kind() != reflect.Uint64 {
				return errors.New("第一个字段必须是insert_id uint64")
			}
			c.InsertIDKey = "insert_id"
		}
		if i == c.Type.NumField()-1 {
			if db != "flag" || field.Type.Kind() != reflect.Uint8 {
				return errors.New("最后一个字段必须是flag uint8")
			}
			c.FlagKey = "flag"
		}
		if db != strx.ToSnakeCase(field.Name) {
			return errors.New(c.Type.Name() + "类型的" + field.Name + "字段，'db'Tag格式不是标准的SnakeCase")
		}
		comment, _ := field.Tag.Lookup("comment")
		_, enum := field.Tag.Lookup("enum")

		// collect info
		c.FieldDBs = append(c.FieldDBs, db)
		c.FieldNames = append(c.FieldNames, field.Name)
		if _, ok := field.Tag.Lookup("index"); ok {
			c.Indexes = append(c.Indexes, db)
		}

		// sql generating
		sqlType, e := c.goTypeToCkType(field.Type, db, enum)
		if e != nil {
			logx.Error(e)
			return e
		}
		c.sqlCreateTable += "`" + db + "` " + sqlType + " COMMENT '" + comment + "',"
		c.sqlAllCreateTable += "`" + db + "` " + sqlType + " COMMENT '" + comment + "',"
		c.sqlInsert += "?,"
	}

	if len(c.Indexes) == 0 {
		return errors.New("必须设置至少一个带index索引的字段")
	}

	c.sqlCreateTable = strings.TrimSuffix(c.sqlCreateTable, ",")
	c.sqlAllCreateTable = strings.TrimSuffix(c.sqlAllCreateTable, ",")
	c.sqlCreateTable += ")ENGINE ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/" + c.Database + "_" + c.TableName + "_" + strconv.Itoa(numx.Randn(100)) + "', '{replica}') PARTITION BY toYYYYMM(" + createTimeDB + ") ORDER BY(" + strings.Join(c.Indexes, ",") + ")SETTINGS index_granularity = 8192"
	c.sqlAllCreateTable += ")ENGINE Distributed(" + c.Cluster + ",'" + c.Database + "','" + c.TableName + "',sipHash64(" + c.Indexes[0] + "))"
	c.sqlNowCreateView = `create view ` + ine + c.Database + "." + c.TableName + "_now on cluster " + c.Cluster + ` as select * from ` + c.Database + "." + c.TableName + "_all where flag=1"
	c.sqlInsert = strings.TrimSuffix(c.sqlInsert, ",")
	c.sqlInsert += ")"

	return nil
}

func (c *ClickHouseClusterModel) checkColumns() error {
	for _, replica := range c.Shards {
		for _, node := range replica {
			conn := clickhouse.New(node)
			columns, e := DescribeTable(conn, c.TableName)
			if e != nil {
				logx.Error(e)
				return e
			}
			if len(c.FieldDBs) != len(columns) {
				return errors.New("线上" + c.TableName + "表的字段数量于struct不一致,节点:" + node)
			}
			for i, column := range columns {
				if c.FieldDBs[i] != column.Name {
					return errors.New(c.Type.Name() + "类型于线上" + c.TableName + "字段不一致：" + c.FieldDBs[i] + "->" + column.Name + ", 节点:" + node)
				}
			}
		}
	}
	return nil
}

func (c *ClickHouseClusterModel) createTableIfNotExists() error {
	_, e := c.Conn.Exec(c.sqlCreateTable)
	if e != nil {
		logx.Error(e)
		println(c.sqlCreateTable)
		return e
	}
	_, e = c.Conn.Exec(c.sqlAllCreateTable)
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = c.Conn.Exec(strings.Replace(c.sqlAllCreateTable, "on cluster "+c.Cluster, " ", -1))
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = c.Conn.Exec(c.sqlNowCreateView)
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = c.Conn.Exec(strings.Replace(c.sqlNowCreateView, "on cluster "+c.Cluster, " ", -1))
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}

func (c *ClickHouseClusterModel) DropTableIfExists() error {
	_, e := c.Conn.Exec(`drop table if exists ` + c.Database + "." + c.TableName + " on cluster " + c.Cluster)
	if e != nil {
		logx.Error(e)
		return e
	}
	all := `drop table if exists ` + c.Database + "." + c.TableName + "_all on cluster " + c.Cluster
	_, e = c.Conn.Exec(all)
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = c.Conn.Exec(strings.Replace(all, "on cluster "+c.Cluster, " ", -1))
	if e != nil {
		logx.Error(e)
		return e
	}
	now := `drop table if exists ` + c.Database + "." + c.TableName + "_now on cluster " + c.Cluster
	_, e = c.Conn.Exec(now)
	if e != nil {
		logx.Error(e)
		return e
	}
	_, e = c.Conn.Exec(strings.Replace(now, "on cluster "+c.Cluster, " ", -1))
	if e != nil {
		logx.Error(e)
		return e
	}
	return nil
}

func (c *ClickHouseClusterModel) goTypeToCkType(t reflect.Type, db string, enum bool) (string, error) {
	switch t.Kind() {
	case reflect.Uint8:
		return "UInt8", nil
	case reflect.Uint16:
		return "UInt16", nil
	case reflect.Uint32:
		return "UInt32", nil
	case reflect.Uint64, reflect.Uint:
		return "UInt64", nil
	case reflect.Int8:
		return "Int8", nil
	case reflect.Int16:
		return "Int16", nil
	case reflect.Int32:
		return "Int32", nil
	case reflect.Int, reflect.Int64:
		return "Int64", nil
	case reflect.Float32:
		return "Float32", nil
	case reflect.Float64:
		return "Float64", nil
	case reflect.String:
		if enum {
			return "LowCardinality(String)", nil
		}
		return "String", nil
	case reflect.Struct:
		switch t.Name() {
		case "Time":
			return "DateTime", nil
		default:
			return "", errors.New("unsupported type name:" + t.Name())
		}
	default:
		return "", errors.New("unsupported type:" + t.Kind().String())
	}
}

func (c *ClickHouseClusterModel) BatchInsert(vs interface{}) error {
	t := reflect.TypeOf(vs)
	if t.Kind() != reflect.Slice {
		return errors.New("BatchInsert必须传入一个切片类型的数据")
	}
	t = t.Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	} else {
		return errors.New("BatchInsert只能传入[]*" + c.Type.Name() + "类型的数据")
	}
	if t.Name() != c.Type.Name() {
		return errors.New("BatchInsert只能传入[]*" + c.Type.Name() + "类型的数据")
	}

	e := c.Conn.Transact(func(session sqlx.Session) error {
		stmt, e := session.Prepare(c.sqlInsert)
		if e != nil {
			return e
		}
		defer stmt.Close()
		values := reflect.ValueOf(vs)
		for j := 0; j < values.Len(); j++ {
			args := []interface{}{}
			value := values.Index(j).Elem()
			for i := 0; i < value.NumField(); i++ {
				field := value.Field(i)
				arg := field.Interface()
				if c.FieldDBs[i] == "insert_id" {
					arg = time.Now().UnixNano() + numx.Rand63n(1000)
				} else if c.FieldDBs[i] == "flag" {
					arg = 1
				}
				args = append(args, arg)
			}
			_, e = stmt.Exec(args...)
			if e != nil {
				return e
			}
		}
		return nil
	})
	return e
}

func (c *ClickHouseClusterModel) QueryWhere(where string, args ...interface{}) (interface{}, error) {
	sqlWhere := ""
	if where != "" {
		sqlWhere = " where " + where
	}
	query := `select ` + strings.Join(c.FieldDBs, ",") + " from " + c.Database + "." + c.TableName + "_now " + sqlWhere
	value := reflect.New(reflect.SliceOf(reflect.PtrTo(c.Type)))
	vs := value.Interface()
	e := c.Conn.QueryRows(vs, query, args...)
	if e != nil {
		return nil, e
	}
	return reflect.ValueOf(vs).Elem().Interface(), nil
}

func (c *ClickHouseClusterModel) All() (interface{}, error) {
	return c.QueryWhere("")
}

func (c *ClickHouseClusterModel) MustAll() interface{} {
	vs, e := c.All()
	if e != nil {
		log.Fatal(e)
	}
	return vs
}

func (c *ClickHouseClusterModel) DeleteWhere(where string, args ...interface{}) error {
	sqlWhere := " where 1"
	if where != "" {
		sqlWhere = " where " + where
	}
	query := `alter table ` + c.Database + "." + c.TableName + " on cluster " + c.Cluster + " delete " + sqlWhere
	_, e := c.Conn.Exec(query, args...)
	return e
}
