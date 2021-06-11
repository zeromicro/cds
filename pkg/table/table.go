package table

import (
	"fmt"
	"strings"
)

const (
	MTLocal = iota
	Distribute
	MvLocal
	MvInner
	MvDistribute
	MvNow
)

type Column struct {
	Name    string
	Type    string
	Comment string
}

type TableMeta struct {
	QueryKey   string
	Columns    []Column
	DB         string
	Table      string
	CreateTime string
	UpdateTime string
	Indexes    string
	M          map[string]int
	Category   string
	WithTime   bool
}

func (meta *TableMeta) buildColumn() string {
	var buf strings.Builder
	buf.Write([]byte("`insert_id` UInt64 COMMENT '插入id unix timestamp nano second',\n"))
	for _, column := range meta.Columns {
		buf.WriteString("`" + column.Name + "` " + column.Type)
		if len(column.Comment) > 0 {
			buf.WriteString(" COMMENT '")
			buf.WriteString(column.Comment)
			buf.WriteByte('\'')
		}
		buf.WriteByte(',')
		buf.WriteByte('\n')
	}
	if meta.WithTime {
		buf.Write([]byte("`__time` DateTime COMMENT '第三方时间戳',\n"))
	}
	buf.Write([]byte("`ck_is_delete` UInt8 COMMENT '用于记录删除状态 0为正常状态 1为删除状态'"))

	return buf.String()
}

func (meta *TableMeta) CreateTable(category int, distribute bool) string {
	columns := meta.buildColumn()
	var buf strings.Builder
	meta.buildHead(category, &buf)
	if distribute {
		buf.WriteString(" ON CLUSTER bip_ck_cluster ")
	}
	if category != MvLocal {
		buf.WriteByte('(')
		buf.WriteString(columns)
		buf.WriteByte(')')
	}

	meta.buildEnd(category, &buf)
	buf.WriteByte('\n')
	return buf.String()
}

// 生成表头，如 create table if not exists `foo`.`bar_full_all`
func (meta *TableMeta) buildHead(category int, buf *strings.Builder) {
	buf.WriteString("CREATE ")
	dbTable := " `" + meta.DB + "`.`" + meta.Table
	switch category {
	case MTLocal:
		buf.WriteString("TABLE IF NOT EXISTS ")
		buf.WriteString(dbTable + "` ")
	case Distribute:
		buf.WriteString("TABLE IF NOT EXISTS ")
		buf.WriteString(dbTable + "_full_all` ")
	case MvInner:
		buf.WriteString("TABLE IF NOT EXISTS ")
		buf.WriteString(" `" + meta.DB + "`.`.rtu_inner." + meta.Table + "_mv` ")
	case MvDistribute:
		buf.WriteString("TABLE IF NOT EXISTS ")
		buf.WriteString(dbTable + "_all` ")
	case MvLocal:
		buf.WriteString(" MATERIALIZED VIEW IF NOT EXISTS ")
		buf.WriteString(dbTable + "_mv` ")
	case MvNow:
		buf.WriteString("VIEW IF NOT EXISTS ")
		buf.WriteString(dbTable + "_now` ")
	}
}

// 生成引擎信息，比如mergeTree 或者ReplacingMergeTree
func (meta *TableMeta) buildEnd(category int, buf *strings.Builder) {
	var partitionString string
	dbTable := meta.DB + "." + meta.Table
	partitionKey := meta.UpdateTime
	if len(partitionKey) == 0 && len(meta.CreateTime) != 0 {
		partitionKey = meta.CreateTime
	}
	if len(partitionKey) != 0 {
		partitionString = fmt.Sprintf("PARTITION BY toYYYYMM(%s)", partitionKey)
	}
	switch category {
	case MTLocal:
		buf.WriteString(
			`ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_` + dbTable + `', '{replica}')
		` + partitionString + `
		ORDER BY (` + meta.QueryKey + `)
		SETTINGS index_granularity = 8192`)
	case Distribute:
		buf.WriteString(`ENGINE = Distributed('bip_ck_cluster', '` + meta.DB + `', '` + meta.Table + `', sipHash64(` + meta.QueryKey + `))`)
	case MvDistribute:
		buf.WriteString(`ENGINE = Distributed('bip_ck_cluster', '` + meta.DB + `', '` + meta.Table + `_mv', sipHash64(` + meta.QueryKey + `))`)
	case MvInner:
		// 生成类似
		//`ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_` + dbTable + `_mv', '{replica}', `__time`)
		//` + partitionString + `
		//ORDER BY ` + meta.QueryKey + `
		//SETTINGS index_granularity = 8192 AS `
		buf.WriteString(`ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_`)
		buf.WriteString(dbTable)
		if meta.WithTime {
			buf.WriteString("_mv', '{replica}',`__time`) ")
		} else {
			buf.WriteString(`_mv', '{replica}')`)
		}
		buf.WriteString(partitionString)
		buf.WriteString("\nORDER BY ")
		buf.WriteString(meta.QueryKey)
		buf.WriteString("\nSETTINGS index_granularity = 8192")
		//buf.WriteString(" AS SELECT * FROM " + dbTable)
	case MvLocal:
		buf.WriteString("TO " + " `" + meta.DB + "`.`.rtu_inner." + meta.Table + "_mv` AS SELECT * from " + dbTable)
	case MvNow:
		buf.WriteString(" AS \n")
		buf.WriteString("SELECT * FROM " + dbTable)
		buf.WriteString("_all")
		buf.WriteString(" FINAL WHERE ck_is_delete = 0")
	}
}
