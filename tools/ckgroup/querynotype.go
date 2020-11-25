package ckgroup

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/tal-tech/go-zero/core/logx"
)

// 不依赖查询的结构体, 返回单条 map[string]interface{}
func (g *dbGroup) QueryRowNoType(v interface{}, query string, args ...interface{}) error {
	return scanRowNoType(g.QueryNode.GetRawConn(), v, query, args...)
}

// 不依赖查询的结构体，直接返回查询结果的slice切片集合 []map[string]interface{}
func (g *dbGroup) QueryRowsNoType(v interface{}, query string, args ...interface{}) error {
	return scanRowsNoType(g.QueryNode.GetRawConn(), v, query, args...)
}

// 不需要具体固定的结构体type，只需要querySQL，把查询的结果组装成map，通过channel返回给caller
func (g *dbGroup) BatchQueryRowsNoType(cnt int, query string, args ...interface{}) (chan interface{}, error) {
	// 通过管道返回查询结果，实现类似流，边输出边处理
	ch := make(chan interface{}, cnt)
	err := BatchScanRowsNoType(g.QueryNode.GetRawConn(), ch, query, args...)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// dest 1.must pointer; 2.must map[string]interface{}, can set Value
func scanRowNoType(db *sql.DB, dest interface{}, query string, args ...interface{}) error {
	destv := reflect.ValueOf(dest)
	if destv.Kind() != reflect.Ptr {
		return fmt.Errorf("non-pointer %v", destv.Type())
	}
	if e := destv.Type().Elem(); e.Kind() != reflect.Map {
		return fmt.Errorf("can't construct and fill non-Map value %v", destv.Type())
	}
	destvMap := reflect.MakeMap(reflect.TypeOf(dest).Elem())

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	columns := make([]string, len(cols))
	columnPtrs := make([]interface{}, len(cols))
	for i := range columns {
		columnPtrs[i] = &columns[i]
	}

	if rows.Next() {
		err := rows.Scan(columnPtrs...)
		if err != nil {
			return err
		}

		for i, col := range cols {
			c := columns[i]
			destvMap.SetMapIndex(reflect.ValueOf(col), reflect.ValueOf(c))
		}
	}
	destv.Elem().Set(destvMap)
	return nil
}

func scanRowsNoType(db *sql.DB, dest interface{}, query string, args ...interface{}) error {
	vdest := reflect.ValueOf(dest)
	// 必须是指针，reflect.ValueOf(dest) 获取到的值才是对应的指针，后续才可以获取指针指向地址的变量
	if vdest.Kind() != reflect.Ptr {
		return fmt.Errorf("non-pointer %v", vdest.Type())
	}
	vdestout := vdest.Elem()
	if vdestout.Kind() != reflect.Slice {
		return fmt.Errorf("can't fill non-slice value")
	}
	if e := vdestout.Type().Elem(); e.Kind() != reflect.Map {
		return fmt.Errorf("can't construct and fill non-Map value %v", vdestout.Type())
	}
	// reflect.TypeOf(dest).Elem() 获取dest的原始 [dest.Type() 等价于 reflect.TypeOf(dest)]
	vdestSlice := reflect.MakeSlice(vdest.Type().Elem(), 0, 0)

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	// 获取返回rows中选定的列：select tag_id, tag_name from hera.user_tags_all -> cols = [tag_id, tag_name]
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	columns := make([]string, len(cols))
	// columnPtrs 存储 columns 中每个column的指针，针对Scan()的赋值
	columnPtrs := make([]interface{}, len(cols))
	for i := range columns {
		columnPtrs[i] = &columns[i]
	}

	for rows.Next() {
		// Scan对columnPtrs赋值的同时会给columns赋值，所以columns中存储的是查询出来的值
		err := rows.Scan(columnPtrs...)
		if err != nil {
			logx.Error(err)
			continue
		}
		// 每次获取到rows_data, 都创建一个map, 做column -> data的映射
		entry := make(map[string]interface{})
		for i, col := range cols {
			c := columns[i]
			entry[col] = c
		}
		vdestSlice = reflect.Append(vdestSlice, reflect.ValueOf(entry))
	}
	// 需要对指针获取它的地址值 .Elem()，才能对其进行赋值
	vdestout.Set(vdestSlice)
	return nil
}

// 仅靠querysql去查询，获取流式结果
func BatchScanRowsNoType(db *sql.DB, ch chan interface{}, query string, args ...interface{}) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	columns := make([]string, len(cols))
	columnPtrs := make([]interface{}, len(cols))
	for i := range columns {
		columnPtrs[i] = &columns[i]
	}

	go func() {
		defer close(ch)

		for rows.Next() {
			err := rows.Scan(columnPtrs...)
			if err != nil {
				logx.Error(err)
				continue
			}

			entry := make(map[string]interface{})
			for i, col := range cols {
				c := columns[i]
				entry[col] = c
			}
			ch <- reflect.ValueOf(entry).Interface()
		}
	}()

	return nil
}
