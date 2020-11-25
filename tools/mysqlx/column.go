package mysqlx

type Column struct {
	Field      string      `db:"Field"`
	Type       string      `db:"Type"`
	Collation  interface{} `db:"Collation"`
	Null       string      `db:"Null"`
	Key        string      `db:"Key"`
	Default    interface{} `db:"Default"`
	Extra      string      `db:"Extra"`
	Privileges string      `db:"Privileges"`
	Comment    string      `db:"Comment"`
}
