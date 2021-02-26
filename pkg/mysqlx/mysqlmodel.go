package mysqlx

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/tal-tech/cds/pkg/strx"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

type (
	MySQLModel struct {
		Conn      sqlx.SqlConn
		AppName   string
		Dsn       string
		Database  string
		TableName string
		Type      reflect.Type
		Indexes   []string

		DBs               []string
		MutableFieldDBs   []string // 字段的tag里面的db:""
		MutableFieldNames []string // 字段的名字

		sqlCreateTable string
		sqlInsert      string
	}
	Count struct {
		Count int64 `db:"count"`
	}
)

// MustNewMySQLModel 按照data结构体来建表，如果已存在则删掉他重新建
func MustNewMySQLModel(appName string, dsn string, data interface{}) (*MySQLModel, bool, error) {
	b := &MySQLModel{
		AppName: appName,
	}
	var e error
	b.Dsn, b.Database, e = ParseMySQLDatabase(dsn)
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	e = CreateDbIne(dsn)
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	b.Conn = TakeMySQLConnx(dsn)

	e = b.initData(data)
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	e = b.DropIfExists()
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	created, e := b.createTableIfNotExists()
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	e = b.tableColumnCheck()
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	b.generateSQLInsert()
	return b, created, nil
}

// NewMySQLModel 新建基础Model，按照data结构体来建表，如果已存在则不建表，但是会检测每一列是否和结构体字段对得上
func NewMySQLModel(appName string, dsn string, data interface{}) (*MySQLModel, bool, error) {
	b := &MySQLModel{
		AppName: appName,
	}
	var e error
	b.Dsn, b.Database, e = ParseMySQLDatabase(dsn)
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	e = CreateDbIne(dsn)
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}
	b.Conn = sqlx.NewMysql(b.Dsn)

	e = b.initData(data)
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}

	created, e := b.createTableIfNotExists()
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}

	e = b.tableColumnCheck()
	if e != nil {
		logx.Error(e)
		return nil, false, e
	}

	b.generateSQLInsert()
	return b, created, nil
}

// initData 如果表不存在，会自动建表，建索引
func (b *MySQLModel) initData(data interface{}) error {
	b.Type = reflect.TypeOf(data)
	if b.Type.Kind() == reflect.Ptr {
		b.Type = b.Type.Elem()
	}
	if b.Type.Kind() == reflect.Slice {
		b.Type = b.Type.Elem()
	}
	if b.Type.Kind() == reflect.Ptr {
		b.Type = b.Type.Elem()
	}
	if b.AppName != "" {
		b.TableName = strx.ToSnakeCase(b.AppName) + "_" + strx.ToSnakeCase(b.Type.Name())
	} else {
		b.TableName = strx.ToSnakeCase(b.Type.Name())
	}

	b.sqlCreateTable = "create table `" + b.Database + "`." + b.TableName + "(\n"
	for i := 0; i < b.Type.NumField(); i++ {
		field := b.Type.Field(i)
		// tag check
		db, ok := field.Tag.Lookup("db")
		if !ok {
			return errors.New(b.Type.Name() + "类型的" + field.Name + "字段没有写'db' Tag")
		}
		// if db != strx.ToSnakeCase(field.Name) {
		// 	return errors.New(b.Type.Name() + "类型的" + field.Name + "字段，'db'Tag格式不是标准的SnakeCase")
		// }
		comment, _ := field.Tag.Lookup("comment")
		length, e := GetLengthTag(field)
		if e != nil {
			return errors.New(b.Type.Name() + "类型的" + field.Name + "字段的'length' Tag格式不正确")
		}

		// collect info
		if _, ok := field.Tag.Lookup("index"); ok {
			b.Indexes = append(b.Indexes, db)
		}

		// sql generating
		mutable, sqlType, e := b.goTypeToMySQLType(field.Type, db, length)
		if e != nil {
			logx.Error(e)
			return e
		}
		if i == 0 && sqlType == "text" {
			sqlType = "varchar(36)"
		}
		b.sqlCreateTable += "`" + db + "` " + sqlType + " "
		if i == 0 {
			if strings.Contains(sqlType, "int") {
				b.sqlCreateTable += `auto_increment `
			}
		} else if !strings.Contains(strings.ToLower(field.Type.Name()), "null") {
			b.sqlCreateTable += `not null `
		}
		if db == `update_time` && field.Type.Name() == "Time" {
			b.sqlCreateTable += ` on update CURRENT_TIMESTAMP `
		}
		b.sqlCreateTable += ` comment '` + comment + `' `
		if i == 0 {
			b.sqlCreateTable += ` primary key `
		}
		b.sqlCreateTable += ",\n"

		if mutable {
			b.MutableFieldDBs = append(b.MutableFieldDBs, db)
			b.MutableFieldNames = append(b.MutableFieldNames, field.Name)
		}
		b.DBs = append(b.DBs, db)
	}

	return nil
}

func (b *MySQLModel) createTableIfNotExists() (bool, error) {
	// sql
	b.sqlCreateTable = strings.TrimSuffix(b.sqlCreateTable, ",\n")
	b.sqlCreateTable += "\n)"
	exists, e := b.tableExists()
	if e != nil {
		return false, e
	}
	if exists {
		return false, nil
	}
	_, e = b.Conn.Exec(b.sqlCreateTable)
	if e != nil {
		return false, e
	}
	e = CreateMysqlIndexes(b.Conn, b.Database, b.TableName, b.Indexes)
	if e != nil {
		logx.Error(e)
		return false, e
	}

	return true, nil
}

func (b *MySQLModel) tableColumnCheck() error {
	columns, e := DescribeMysqlTable(b.Conn, b.TableName)
	if e != nil {
		return e
	}
	if len(columns) > len(b.DBs) {
		return errors.New("线上" + b.TableName + "表的字段数比" + b.Type.Name() + "类型字段多")
	}
	for i, column := range columns {
		if b.DBs[i] != column.Field {
			return errors.New(b.Type.Name() + "类型与线上" + b.TableName + "表字段不一致：" + b.DBs[i] + "->" + column.Field)
		}
	}
	return nil
}

func (b *MySQLModel) generateSQLInsert() {
	b.sqlInsert = `insert into ` + b.TableName + ` (` + strings.Join(b.MutableFieldDBs, ",") + `) values(
		` + strings.Join(strx.SlicifyStr("?", len(b.MutableFieldDBs)), ",") + `
		)`
}

func (b *MySQLModel) Insert(data interface{}) (int64, error) {
	args := []interface{}{}
	value := reflect.ValueOf(data)
	if value.Type().Kind() == reflect.Slice {
		return 0, errors.New("data类型为slice，Insert仅支持单条插入，批量插入请使用BatchInsert()")
	}
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	for _, name := range b.MutableFieldNames {
		v := value.FieldByName(name).Interface()
		args = append(args, v)
	}

	result, e := b.Conn.Exec(b.sqlInsert, args...)
	if e != nil {
		return 0, e
	}
	return result.LastInsertId()
}

// BatchInsert 批量插入
func (b *MySQLModel) BatchInsert(vs interface{}) error {
	t := reflect.TypeOf(vs)
	if t.Kind() != reflect.Slice {
		return errors.New("BatchInsert必须传入一个切片类型的数据")
	}
	t = t.Elem()
	ptrMode := false
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		ptrMode = true
	}
	if t.Name() != b.Type.Name() {
		return errors.New("BatchInsert只能传入[]*" + b.Type.Name() + "类型的数据")
	}

	e := b.Conn.Transact(func(session sqlx.Session) error {
		stmt, e := session.Prepare(b.sqlInsert)
		if e != nil {
			return e
		}
		defer stmt.Close()
		values := reflect.ValueOf(vs)
		for i := 0; i < values.Len(); i++ {
			args := []interface{}{}
			value := values.Index(i)
			if ptrMode {
				value = value.Elem()
			}
			for _, name := range b.MutableFieldNames {
				args = append(args, value.FieldByName(name).Interface())
			}
			_, e = stmt.Exec(args...)
			if e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	return nil
}

// Drop drop table
func (b *MySQLModel) Drop() error {
	sql := `drop table ` + b.TableName
	_, e := b.Conn.Exec(sql)
	return e
}

func (b *MySQLModel) DropIfExists() error {
	sql := `drop table if exists ` + b.TableName
	_, e := b.Conn.Exec(sql)
	return e
}

func (b *MySQLModel) FindBy(field string, fieldValue interface{}) (interface{}, error) {
	if !strx.SliceContains(b.DBs, field) {
		return nil, errors.New(`field ` + field + ` doesn't exists`)
	}

	query := `select ` + strings.Join(b.DBs, ",") + ` from ` + b.TableName + ` where ` + field + `=?`
	v := reflect.New(b.Type).Interface()
	e := b.Conn.QueryRow(v, query, fieldValue)
	if e != nil {
		return nil, e
	}
	return v, nil
}

func (b *MySQLModel) FindWhere(where string, args ...interface{}) (interface{}, error) {
	sqlWhere := ""
	if where != "" {
		sqlWhere = " where " + where
	}
	query := `select ` + strings.Join(b.DBs, ",") + " from " + b.TableName + sqlWhere
	v := reflect.New(b.Type).Interface()
	e := b.Conn.QueryRow(v, query, args...)
	if e != nil {
		return nil, e
	}
	return v, nil
}

/* QueryWhere 按条件查询

where : 查询条件，如果为空则没有查询条件。如：where="id=1" -> `select .. from table where id=1` . where="" -> `select .. from table`

返回：结构体的指针切片类型的interface{}，如：[]*User类型
*/
func (b *MySQLModel) QueryWhere(where string, args ...interface{}) (interface{}, error) {
	sqlWhere := ""
	if where != "" {
		sqlWhere = " where " + where
	}
	query := `select ` + strings.Join(b.DBs, ",") + ` from ` + b.TableName + sqlWhere
	value := reflect.New(reflect.SliceOf(reflect.PtrTo(b.Type)))
	vs := value.Interface()
	e := b.Conn.QueryRows(vs, query, args...)
	if e != nil {
		return nil, e
	}
	return reflect.ValueOf(vs).Elem().Interface(), nil
}

func (b *MySQLModel) DeleteWhere(where string, args ...interface{}) (int64, error) {
	sqlWhere := ""
	if where != "" {
		sqlWhere = " where " + where
	}
	query := `delete from ` + b.TableName + sqlWhere
	result, e := b.Conn.Exec(query, args...)
	if e != nil {
		return 0, e
	}
	return result.RowsAffected()
}

func (b *MySQLModel) Update(id interface{}, sets string, args ...interface{}) (int64, error) {
	query := `update ` + b.TableName + ` set ` + sets + ` where ` + b.DBs[0] + `=?`
	result, e := b.Conn.Exec(query, append(args, id)...)
	if e != nil {
		return 0, e
	}
	return result.RowsAffected()
}

func (m *MySQLModel) UpdateWhere(sets string, where string, args ...interface{}) (int64, error) {
	sqlWhere := ""
	if where != "" {
		sqlWhere = " where " + where
	}
	query := `update ` + m.TableName + ` set ` + sets + sqlWhere
	result, e := m.Conn.Exec(query, args...)
	if e != nil {
		return 0, e
	}
	return result.RowsAffected()
}

func (b *MySQLModel) Delete(id interface{}) (int64, error) {
	query := `delete from ` + b.TableName + ` where ` + b.MutableFieldDBs[0] + `=?`
	result, e := b.Conn.Exec(query, id)
	if e != nil {
		return 0, e
	}
	return result.RowsAffected()
}

func (b *MySQLModel) Count(where string, args ...interface{}) (int64, error) {
	c := Count{}
	if where != "" {
		where = ` where ` + where
	}
	e := b.Conn.QueryRow(&c, "select count(1) as `count` from "+b.TableName+where, args...)
	if e != nil {
		logx.Error(e)
		return 0, e
	}
	return c.Count, nil
}

func GetLengthTag(field reflect.StructField) (int, error) {
	length, ok := field.Tag.Lookup("length")
	if !ok {
		return 0, nil
	}
	return strconv.Atoi(length)
}

func (b *MySQLModel) goTypeToMySQLType(t reflect.Type, db string, length int) (bool, string, error) {
	switch t.Name() {
	case "string":
		if t.Name() == "string" {
			if length == 0 {
				return true, "text", nil
			}
			return true, "varchar(" + strconv.Itoa(length) + ")", nil
		}
	case "Time":
		if db == "create_time" || db == "update_time" {
			return false, "timestamp NOT NULL default CURRENT_TIMESTAMP", nil
		}
		return true, "timestamp NOT NULL", nil
	case "NullTime":
		return true, "timestamp NULL", nil
	}

	if strings.Contains(t.Name(), "uint") {
		if length != 0 {
			return true, "bigint(" + strconv.Itoa(length) + ") unsigned", nil
		}
		return true, "bigint unsigned", nil
	}

	if strings.Contains(t.Name(), "int") {
		if length != 0 {
			return true, "bigint(" + strconv.Itoa(length) + ")", nil
		}
		return true, "bigint", nil
	}
	if strings.Contains(t.Name(), "float") {
		if length != 0 {
			return true, "float(" + strconv.Itoa(length) + ")", nil
		}
		return true, "float", nil
	}
	return true, "", errors.New("unsupported Go type:" + t.Name())
}

func (b *MySQLModel) tableExists() (bool, error) {
	_, e := b.Conn.Exec("describe `" + b.TableName + "`")
	if e != nil {
		if strings.Contains(e.Error(), `Table`) && strings.Contains(e.Error(), `doesn't exist`) {
			return false, nil
		}
		return false, e
	}
	return true, nil
}

func (m *MySQLModel) All() (interface{}, error) {
	return m.QueryWhere("")
}

func (m *MySQLModel) MustAll() interface{} {
	vs, e := m.All()
	if e != nil {
		log.Fatal(e)
	}
	return vs
}
