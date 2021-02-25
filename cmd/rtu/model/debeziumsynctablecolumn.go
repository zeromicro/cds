package model

import "database/sql"

// SQLTable 记录 clickhouse table 各种信息
type SQLTable struct {
	DbName              string
	Table               string
	Types               map[string]DataType //列名和类型的映射
	PrimaryKey          string
	Columns             []string      //列信息（有序）
	ColumnsDefaultValue []interface{} // 用作nil填充
	PrimaryKeyIndex     int

	InsertSQL string
	QuerySQL  string
	QueryNode *sql.DB
}
