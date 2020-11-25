package ckgroup

import (
	"database/sql"

	"github.com/tal-tech/go-zero/core/logx"

	"reflect"
)

type CKConn interface {
	GetHost() string
	GetUser() string
	GetRawConn() *sql.DB
	Exec(query string, args ...interface{}) error
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

func NewCKConn(dns string) (CKConn, error) {
	db, err := sql.Open(DRIVER, dns)
	if err != nil {
		return nil, err
	}
	host, user, err := parseHostAndUser(dns)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &ckConn{
		Host: host,
		Conn: db,
		User: user,
	}, nil
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

	return saveData(client.Conn, insertSQL, argss)
}
