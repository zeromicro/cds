package clickhousex

import (
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type Column struct {
	Name              string `db:"name"`
	Type              string `db:"type"`
	DefaultValue      string `db:"default_value"`
	DefaultExpression string `db:"default_expression"`
	Comment           string `db:"comment"`
	CodecExpression   string `db:"codec_expression"`
	TTLExpression     string `db:"ttl_expression"`
}

// DescribeTable 新建clickhouse table信息
func DescribeTable(conn sqlx.SqlConn, table string) ([]*Column, error) {
	columns := []*Column{}
	e := conn.QueryRows(&columns, `describe `+table)
	if e != nil {
		return nil, e
	}
	return columns, nil
}

func ColumnsMatch(cs1, cs2 []*Column) bool {
	if len(cs1) != len(cs2) {
		return false
	}
	for i := 0; i < len(cs1); i++ {
		if cs1[i].Name != cs2[i].Name {
			return false
		}
	}
	return true
}
