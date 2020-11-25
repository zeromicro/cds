package clickhousex

import "testing"

func Test_generateTempSql(t *testing.T) {
	sql := `CREATE TABLE hera.user ON CLUSTER bip_ck_cluster
	(
	  insert_id        UInt64 COMMENT '插入id unix timestamp nano second',
	  id          String COMMENT '晓黑板用户id',
	  real_name        String COMMENT '真实姓名',
	  role             LowCardinality(String) COMMENT '用户角色',
	  mobile           String COMMENT '手机号',
	  account_source   LowCardinality(String) COMMENT '账号来源，默认为晓黑板',
	  address          String COMMENT '地址',
	  create_time      DateTime COMMENT '用户创建时间',
	  update_time      DateTime COMMENT '晓黑板user表记录更新时间',
	  activation_time  UInt64 COMMENT '激活时间/用户首次使用的时间',
	  invited_number   LowCardinality(String) COMMENT '邀请码',
	  invited_type     UInt8 COMMENT '邀请码类型，0未绑定，1运营（666），2代理（777），3培训师（888），4名师（999），5特殊（150608），6自发生长',
	  province         LowCardinality(String) COMMENT '省',
	  city             LowCardinality(String) COMMENT '市',
	  school           LowCardinality(String) COMMENT '学校名称',
	  wechat_id        LowCardinality(String) COMMENT '微信运营编号',
	  wechat_bind_time DateTime COMMENT '微信运营编号绑定时间',
	  flag             UInt8 COMMENT '本条记录的状态 0刚插入 1当前状态 2过去状态 3已删除'
	) ENGINE ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/k2k_user_1','{replica}') PARTITION BY toYYYYMM(create_time) ORDER BY(role,province,city,create_time,id) SETTINGS index_granularity = 8192;`
	s := `CREATE TABLE hera.user_temp(
	  insert_id        UInt64 COMMENT '插入id unix timestamp nano second',
	  id          String COMMENT '晓黑板用户id',
	  real_name        String COMMENT '真实姓名',
	  role             LowCardinality(String) COMMENT '用户角色',
	  mobile           String COMMENT '手机号',
	  account_source   LowCardinality(String) COMMENT '账号来源，默认为晓黑板',
	  address          String COMMENT '地址',
	  create_time      DateTime COMMENT '用户创建时间',
	  update_time      DateTime COMMENT '晓黑板user表记录更新时间',
	  activation_time  UInt64 COMMENT '激活时间/用户首次使用的时间',
	  invited_number   LowCardinality(String) COMMENT '邀请码',
	  invited_type     UInt8 COMMENT '邀请码类型，0未绑定，1运营（666），2代理（777），3培训师（888），4名师（999），5特殊（150608），6自发生长',
	  province         LowCardinality(String) COMMENT '省',
	  city             LowCardinality(String) COMMENT '市',
	  school           LowCardinality(String) COMMENT '学校名称',
	  wechat_id        LowCardinality(String) COMMENT '微信运营编号',
	  wechat_bind_time DateTime COMMENT '微信运营编号绑定时间',
	  flag             UInt8 COMMENT '本条记录的状态 0刚插入 1当前状态 2过去状态 3已删除'
	) ENGINE ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/k2k_user_2','{replica}') PARTITION BY toYYYYMM(create_time) ORDER BY(role,province,city,create_time,id) SETTINGS index_granularity = 8192;`
	out := generateTempSql(sql, "hera", "user", 1)
	if out != s {
		t.Error("out is not s , but ", out)
		return
	}
}
