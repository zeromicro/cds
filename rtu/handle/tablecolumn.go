package handle

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tal-tech/cds/rtu/model"

	"github.com/tal-tech/go-zero/core/logx"
)

// GetClickhouseTableColumn 获取 clickhouse 列信息
func GetClickhouseTableColumn(db *sql.DB, dbName, table, pk, dbType string) (*model.SQLTable, error) {
	if table == "" {
		return nil, errors.New("GetClickhouseTableColumn: table cannot be empty")
	}
	if dbName == "" {
		return nil, errors.New("GetClickhouseTableColumn: dbName cannot be empty")
	}

	sqlTable := &model.SQLTable{Table: table, DbName: dbName, Types: make(map[string]model.DataType), PrimaryKey: pk}
	query := `describe ` + sqlTable.Table
	if !strings.Contains(query, ".") {
		query = `describe ` + sqlTable.DbName + "." + sqlTable.Table
	}

	datas, e := QueryRows(db, make(map[string]model.DataType), query)
	if e != nil {
		logx.Error(e)
		return nil, e
	}

	for index, data := range datas {
		k := data.Get("name").(string)
		if k == "" {
			continue
		}
		if k == sqlTable.PrimaryKey {
			sqlTable.PrimaryKeyIndex = index
		}
		ckType := data.Get("type").(string)

		tp := model.ParseTypeByCkType(ckType)
		sqlTable.Types[k] = tp
		sqlTable.Columns = append(sqlTable.Columns, k)

		if v, ok := model.NullValMap[tp]; ok {
			sqlTable.ColumnsDefaultValue = append(sqlTable.ColumnsDefaultValue, v)
		} else {
			err := fmt.Errorf("超出预期的ck类型:%s", ckType)
			logx.Error(err)
			return nil, err
		}
	}
	sqlTable.Types["ck_is_delete"] = model.DataTypeInt

	if sqlTable.PrimaryKey == "" {
		for index, data := range datas {
			k := data.Get("name").(string)
			if k == "" {
				continue
			}
			if k == "insert_id" {
				continue
			}
			if strings.Contains(k, "id") || strings.Contains(k, "token") {
				sqlTable.PrimaryKey = k
				sqlTable.PrimaryKeyIndex = index
				break
			}
		}
	}

	if sqlTable.PrimaryKey == "" {
		return nil, errors.New(table + " primary key not found")
	}
	sqlTable.InsertSQL = initInsertSQL(sqlTable)
	sqlTable.QuerySQL = generateQuerySQLTpl(sqlTable)
	return sqlTable, nil
}
func initInsertSQL(table *model.SQLTable) string {
	tmpQ := make([]string, 1, len(table.Columns))
	tmpQ[0] = "?"
	buf := new(bytes.Buffer)
	buf.WriteString("insert into ")
	buf.WriteString(table.DbName + "." + table.Table)
	buf.WriteString("(`")
	buf.WriteString(table.Columns[0])
	for _, columnName := range table.Columns[1:] {
		buf.WriteString("`,`")
		buf.WriteString(columnName)
		tmpQ = append(tmpQ, "?")
	}
	buf.WriteString("`) values (")
	buf.WriteString(strings.Join(tmpQ, ","))
	buf.WriteByte(')')
	return buf.String()
}

// 生成查询sql
func generateQuerySQLTpl(table *model.SQLTable) string {
	sqlTpl := "select %s from %s.%s where `%s` in (%s)"
	strs := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		strs = append(strs, "`"+column+"`")
	}
	columns := strings.Join(strs, ",")
	return fmt.Sprintf(sqlTpl, columns, table.DbName, table.Table, table.PrimaryKey, "%s")
}
