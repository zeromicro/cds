package handle

import (
	"database/sql"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/zeromicro/cds/cmd/rtu/model"
)

// QueryRows 关系型数据库多行查询
func QueryRows(db *sql.DB, types map[string]model.DataType, query string, args ...interface{}) ([]*model.Data, error) {
	return doQuery(db, types, query, false, args...)
}

func doQuery(db *sql.DB, types map[string]model.DataType, query string, singleLine bool, args ...interface{}) ([]*model.Data, error) {
	rows, e := db.Query(query, args...)
	if e != nil {
		logx.Error(e, query)
		return nil, e
	}
	columns, e := rows.Columns()
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	datas := []*model.Data{}
	for rows.Next() {
		e := rows.Scan(scanArgs...)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		sdata := model.NewData()
		for i, col := range values {
			key := columns[i]
			if col == nil {
				sdata.Set(key, nil)
				continue
			}
			v, e := model.ParseSQLValueByType(types[key], string(col))
			if e != nil {
				logx.Error(e)
				return nil, e
			}
			sdata.Set(key, v)
		}
		datas = append(datas, sdata)
		if singleLine {
			break
		}
	}
	return datas, nil
}
