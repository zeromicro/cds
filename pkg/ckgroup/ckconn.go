package ckgroup

import (
	"database/sql"
	"errors"
	"reflect"
	"time"

	"github.com/tal-tech/go-zero/core/logx"
)

type CKConn interface {
	GetHost() string
	GetUser() string
	GetRawConn() *sql.DB
	Exec(query string, args ...interface{}) error
	QueryRowNoType(query string, args ...interface{}) (map[string]interface{}, error)
	QueryRowsNoType(query string, args ...interface{}) ([]map[string]interface{}, error)
	QueryRow(v interface{}, query string, args ...interface{}) error
	QueryRows(v interface{}, query string, args ...interface{}) error
	// QueryStream 流式查询 , 利用 chan 来存储查询的数据
	// chanData 类型只能是 chan *sturct 或 chan sturct
	QueryStream(chanData interface{}, query string, args ...interface{}) error
	// Insert
	// query  形如 insert into user (id,real_name,city) values (#{id},#{real_name},#{city}) , #{}内的字符只能是大小写字母,数字和下划线
	// sliceData  要输入的数组 , 类型只能是 []*sturct 或 []struct
	Insert(query string, sliceData interface{}) error
}

type ckConn struct {
	Host string
	User string
	Conn *sql.DB
}

var hostParseErr = errors.New("parse clickhouse dsn error")

const (
	maxConn         = 10
	connMaxLifetime = 2 * time.Minute
)

func NewCKConn(dns string) (CKConn, error) {
	db, err := sql.Open(DRIVER, dns)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetMaxOpenConns(maxConn)
	host, user, err := parseHostAndUser(dns)
	if err != nil {
		return nil, hostParseErr
	}
	conn := &ckConn{
		Host: host,
		Conn: db,
		User: user,
	}
	if err := db.Ping(); err != nil {
		// 依然返回，防止一个 shard 只有一个节点可用的情况
		return conn, err
	}
	return conn, nil
}

func MustCKConn(dns string) CKConn {
	conn, err := NewCKConn(dns)
	panicIfErr(err)
	return conn
}

func (client *ckConn) GetHost() string {
	return client.Host
}

func (client *ckConn) GetRawConn() *sql.DB {
	return client.Conn
}

func (client *ckConn) GetUser() string {
	return client.User
}

func (client *ckConn) Exec(query string, args ...interface{}) error {
	_, err := client.Conn.Exec(query, args...)
	return err
}

func (client *ckConn) QueryRow(v interface{}, query string, args ...interface{}) error {
	outerPtrType := reflect.TypeOf(v)
	if outerPtrType.Kind() != reflect.Ptr {
		return queryRowTypeErr
	}
	itemType := outerPtrType.Elem()
	if itemType.Kind() != reflect.Struct {
		return queryRowTypeErr
	}

	rows, err := client.Conn.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return err
	}

	columnIdx := make([]int, len(names))
	for i := range columnIdx {
		columnIdx[i] = -1
	}

	for i := 0; i < itemType.NumField(); i++ {
		tagValue := itemType.Field(i).Tag.Get(DbTag)
		for nameIdx, name := range names {
			if tagValue == name {
				columnIdx[nameIdx] = i
			}
		}
	}

	if rows.Next() {
		err := rows.Scan(span(v, columnIdx)...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (client *ckConn) QueryRows(v interface{}, query string, args ...interface{}) error {
	outerPtrType := reflect.TypeOf(v)
	if outerPtrType.Kind() != reflect.Ptr {
		return queryRowsTypeErr
	}
	sliceType := outerPtrType.Elem()
	if sliceType.Kind() != reflect.Slice {
		return queryRowsTypeErr
	}
	innerPtrType := sliceType.Elem()
	if innerPtrType.Kind() != reflect.Ptr {
		return queryRowsTypeErr
	}
	itemType := innerPtrType.Elem()
	if itemType.Kind() != reflect.Struct {
		return queryRowsTypeErr
	}

	rows, err := client.Conn.Query(query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return err
	}
	columnIdx := make([]int, len(names))
	for i := range columnIdx {
		columnIdx[i] = -1
	}

	for i := 0; i < itemType.NumField(); i++ {
		tagValue := itemType.Field(i).Tag.Get(DbTag)
		for nameIdx, name := range names {
			if tagValue == name {
				columnIdx[nameIdx] = i
			}
		}
	}
	result := reflect.MakeSlice(sliceType, 0, 0)

	for rows.Next() {
		v := reflect.New(itemType)
		err := rows.Scan(span(v.Interface(), columnIdx)...)
		if err != nil {
			return err
		}
		result = reflect.Append(result, v)
	}

	destValue := reflect.ValueOf(v)
	destValue.Elem().Set(result)

	return nil
}

func (client *ckConn) QueryStream(dataChan interface{}, query string, args ...interface{}) error {
	chVal := reflect.ValueOf(dataChan)
	if chVal.Kind() != reflect.Chan {
		return chanTpyeErr
	}
	if isChanClosed(dataChan) {
		return chanClosedErr
	}
	dataType := chVal.Type().Elem()
	isPtr := false
	switch dataType.Kind() {
	case reflect.Ptr:
		isPtr = true
		if dataType.Elem().Kind() == reflect.Struct {
			dataType = dataType.Elem()
		} else {
			return chanTpyeErr
		}
	case reflect.Struct:
	default:
		return chanTpyeErr
	}

	rows, err := client.Conn.Query(query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			chVal.Close()
			return nil
		}
		return err
	}
	names, err := rows.Columns()
	if err != nil {
		return err
	}
	columnIdx := make([]int, len(names))
	for i := range columnIdx {
		columnIdx[i] = -1
	}

	for i := 0; i < dataType.NumField(); i++ {
		tagValue := dataType.Field(i).Tag.Get(DbTag)
		for nameIdx, name := range names {
			if tagValue == name {
				columnIdx[nameIdx] = i
			}
		}
	}
	go func() {
		defer chVal.Close()
		defer rows.Close()

		for rows.Next() {
			v := reflect.New(dataType)
			err := rows.Scan(span(v.Interface(), columnIdx)...)
			if err != nil {
				logx.Error(err)
				break
			}
			if isPtr {
				chVal.Send(v)
			} else {
				chVal.Send(v.Elem())
			}
		}
	}()
	return nil
}

func (client *ckConn) Insert(query string, sliceData interface{}) error {
	if containsComment(query) {
		return errors.New("comments are not allowed")
	}
	outerType := reflect.TypeOf(sliceData)
	if outerType.Kind() != reflect.Slice {
		return insertTypeErr
	}
	sliceType := outerType.Elem()

	switch sliceType.Kind() {
	case reflect.Ptr:
		if sliceType.Elem().Kind() != reflect.Struct {
			return insertTypeErr
		}
	case reflect.Struct:
	default:
		return insertTypeErr
	}

	insertSQL, tags := generateInsertSQL(query)
	sliceVal := reflect.ValueOf(sliceData)
	var argss [][]interface{}
	for i := 0; i < sliceVal.Len(); i++ {
		itemVal := sliceVal.Index(i)
		if itemVal.Kind() == reflect.Ptr {
			itemVal = itemVal.Elem()
		}
		args, err := generateRowValue(itemVal, tags)
		if err != nil {
			return err
		}
		argss = append(argss, args)
	}
	if len(argss) == 0 {
		return nil
	}
	now := time.Now()
	err := saveData(client.Conn, insertSQL, argss)
	db, table := parseInsertSQLTableName(insertSQL)
	isSuccess := ""
	if err == nil {
		isSuccess = "1"
	} else {
		isSuccess = "0"
	}
	label := getInsertLabel(db, table, client.Host, isSuccess)
	insertDuHis.With(label).Observe(float64(time.Since(now).Milliseconds()))
	insertBatchSizeGa.With(label).Set(float64(len(argss)))
	insertBatchSizeHis.With(label).Observe(float64(len(argss)))
	return err
}

func (client *ckConn) QueryRowNoType(query string, args ...interface{}) (map[string]interface{}, error) {
	rows, err := client.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if rows.Next() {
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = &values[i]
		}
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}
		m := make(map[string]interface{})
		for i, col := range cols {
			m[col] = values[i]
		}
		return m, nil
	}
	return nil, sql.ErrNoRows
}

func (client *ckConn) QueryRowsNoType(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := client.Conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var result []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = &values[i]
		}
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}
		m := make(map[string]interface{})
		for i, col := range cols {
			m[col] = values[i]
		}
		result = append(result, m)
	}
	return result, nil
}
