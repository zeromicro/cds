package table

import (
	"strings"
	"testing"
)

func TestTable(t *testing.T) {
	mt := "CREATE TABLE IF NOT EXISTS `aa`.`bb` ON CLUSTER bip_ck_cluster\n(\n`insert_id` UInt64 COMMENT '插入id unix timestamp nano second',\n        \n        `aa` int ,\n        `ck_is_delete` UInt8 \tCOMMENT '用于记录删除状态 0为正常状态 1为删除状态'\n        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_aa.bb',\n\t\t\t '{replica}') PARTITION BY toYYYYMM(updateTime) ORDER BY(_id) SETTINGS index_granularity = 8192"
	mv := "CREATEMATERIALIZEDVIEWIFNOTEXISTS`aa`.`bb_mv`ONCLUSTERbip_ck_clusterTO`aa`.`.rtu_inner.bb_mv`ASSELECT*fromaa.bb"
	mvNow := "CREATEVIEWIFNOTEXISTS`aa`.`bb_now`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ASSELECT*FROMaa.bb_allFINALWHEREck_is_delete=0\n"
	mvAll := "CREATETABLEIFNOTEXISTS`aa`.`bb_all`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ENGINE=Distributed('bip_ck_cluster','aa','bb_mv',sipHash64(_id))\n"
	all := "CREATETABLEIFNOTEXISTS`aa`.`bb_full_all`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ENGINE=Distributed('bip_ck_cluster','aa','bb',sipHash64(_id))\n"
	mvInner := "CREATETABLEIFNOTEXISTS`aa`.`.rtu_inner.bb_mv`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_aa.bb_mv','{replica}')PARTITIONBYtoYYYYMM(updateTime)ORDERBY_idSETTINGSindex_granularity=8192"
	Columns := []Column{{"aa", "int", ""}}

	d := &TableMeta{
		QueryKey:   "_id",
		Columns:    Columns,
		DB:         "aa",
		Table:      "bb",
		CreateTime: "createTime",
		UpdateTime: "updateTime",
		Indexes:    "_id",
		M:          nil,
	}
	cases := []struct {
		target    string
		distribue bool
		category  int
	}{
		{target: mt, distribue: true, category: MTLocal},
		{target: mv, distribue: true, category: MvLocal},
		{target: mvAll, distribue: true, category: MvDistribute},
		{target: mvNow, distribue: true, category: MvNow},
		{target: all, distribue: true, category: Distribute},
		{target: mvInner, distribue: true, category: MvInner},
	}

	for index, Case := range cases {
		if removeNewLine(d.CreateTable(Case.category, Case.distribue)) != removeNewLine(Case.target) {
			t.Log("result", removeNewLine(d.CreateTable(Case.category, Case.distribue)))
			t.Log("case  ", removeNewLine(Case.target))
			t.Fatalf("case %d failed", index)
		}
	}
}

func TestTableWithTime(t *testing.T) {
	mt := "CREATE TABLE IF NOT EXISTS `aa`.`bb` ON CLUSTER bip_ck_cluster\n(\n`insert_id` UInt64 COMMENT '插入id unix timestamp nano second',\n        \n        `aa` int ,`__time`DateTimeCOMMENT'第三方时间戳',\n        `ck_is_delete` UInt8 \tCOMMENT '用于记录删除状态 0为正常状态 1为删除状态'\n        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_aa.bb',\n\t\t\t '{replica}') PARTITION BY toYYYYMM(updateTime) ORDER BY(_id) SETTINGS index_granularity = 8192"
	mv := "CREATEMATERIALIZEDVIEWIFNOTEXISTS`aa`.`bb_mv`ONCLUSTERbip_ck_clusterTO`aa`.`.rtu_inner.bb_mv`ASSELECT*fromaa.bb"
	mvNow := "CREATEVIEWIFNOTEXISTS`aa`.`bb_now`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`__time`DateTimeCOMMENT'第三方时间戳',`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ASSELECT*FROMaa.bb_allFINALWHEREck_is_delete=0\n"
	mvAll := "CREATETABLEIFNOTEXISTS`aa`.`bb_all`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`__time`DateTimeCOMMENT'第三方时间戳',`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ENGINE=Distributed('bip_ck_cluster','aa','bb_mv',sipHash64(_id))\n"
	all := "CREATETABLEIFNOTEXISTS`aa`.`bb_full_all`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`__time`DateTimeCOMMENT'第三方时间戳',`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ENGINE=Distributed('bip_ck_cluster','aa','bb',sipHash64(_id))\n"
	mvInner := "CREATETABLEIFNOTEXISTS`aa`.`.rtu_inner.bb_mv`ONCLUSTERbip_ck_cluster(`insert_id`UInt64COMMENT'插入idunixtimestampnanosecond',`aa`int,`__time`DateTimeCOMMENT'第三方时间戳',`ck_is_delete`UInt8COMMENT'用于记录删除状态0为正常状态1为删除状态')ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/blackhole_aa.bb_mv','{replica}',`__time`)PARTITIONBYtoYYYYMM(updateTime)ORDERBY_idSETTINGSindex_granularity=8192\n"

	Columns := []Column{{"aa", "int", ""}}

	d := &TableMeta{
		QueryKey:   "_id",
		Columns:    Columns,
		DB:         "aa",
		Table:      "bb",
		CreateTime: "createTime",
		UpdateTime: "updateTime",
		Indexes:    "_id",
		M:          nil,
		WithTime:   true,
	}
	cases := []struct {
		target    string
		distribue bool
		category  int
	}{
		{target: mt, distribue: true, category: MTLocal},
		{target: mv, distribue: true, category: MvLocal},
		{target: mvAll, distribue: true, category: MvDistribute},
		{target: mvNow, distribue: true, category: MvNow},
		{target: all, distribue: true, category: Distribute},
		{target: mvInner, distribue: true, category: MvInner},
	}

	for index, Case := range cases {
		if removeNewLine(d.CreateTable(Case.category, Case.distribue)) != removeNewLine(Case.target) {
			t.Log("result", removeNewLine(d.CreateTable(Case.category, Case.distribue)))
			t.Log("case  ", removeNewLine(Case.target))
			t.Fatalf("case %d failed", index)
		}
	}
}

func removeNewLine(s string) string {
	for _, i := range []string{"\n", "\r", "\t", " "} {
		s = strings.ReplaceAll(s, i, "")
	}
	return s
}
