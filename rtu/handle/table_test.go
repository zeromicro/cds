package handle

import (
	"testing"

	"github.com/tal-tech/cds/rtu/model"
)

func TestInitInsertSql(t *testing.T) {
	table := model.SQLTable{
		DbName:              "hera",
		Table:               "user_tags",
		Types:               nil,
		PrimaryKey:          "",
		Columns:             []string{"a", "B", "c"},
		ColumnsDefaultValue: nil,
		PrimaryKeyIndex:     0,
		InsertSQL:           "",
		QuerySQL:            "",
		QueryNode:           nil,
	}

	res := initInsertSQL(&table)
	t.Log(res)
	if res != "insert into hera.user_tags(`a`,`B`,`c`) values (?,?,?)" {
		t.Fail()
	}
}

func TestGenerateQuerySQL(t *testing.T) {
	table := model.SQLTable{
		DbName:              "hera",
		Table:               "user_tags",
		Types:               nil,
		PrimaryKey:          "a",
		Columns:             []string{"a", "B", "c"},
		ColumnsDefaultValue: nil,
		PrimaryKeyIndex:     0,
		InsertSQL:           "",
		QuerySQL:            "",
		QueryNode:           nil,
	}

	res := generateQuerySQLTpl(&table)
	t.Log(res)
	if res != "select `a`,`B`,`c` from hera.user_tags where `a` in (%s)" {
		t.Fail()
	}
}
