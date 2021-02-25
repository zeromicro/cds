create database if not exists test on cluster bip_ck_cluster;
create database if not exists test;

create table if not exists test.test_data on cluster bip_ck_cluster
(
  insert_id        UInt64 COMMENT '插入id unix timestamp nano second',
  object_id String,
  pk UInt64,
  int_value UInt64,
  float_value Float32,
  double_value Float64,
  char_value String,
  varchar_value String,
  time_value DateTime,
  creare_time DateTime default now(),
  flag UInt8 COMMENT '本条记录的状态 0刚插入 1当前状态 2过去状态 3已删除'
)ENGINE ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/test',
           '{replica}') PARTITION BY toYYYYMMDD(creare_time) order by pk SETTINGS index_granularity = 8192;

create table if not exists test.test_data_all on cluster bip_ck_cluster
(
  insert_id        UInt64 COMMENT '插入id unix timestamp nano second',
  object_id String,
  pk UInt64,
  int_value UInt64,
  float_value Float32,
  double_value Float64,
  char_value String,
  varchar_value String,
  time_value DateTime,
  creare_time DateTime default now(),
  flag UInt8 COMMENT '本条记录的状态 0刚插入 1当前状态 2过去状态 3已删除'
)ENGINE Distributed(bip_ck_cluster, 'test', 'test_data', sipHash64(pk));

create database if not exists test on cluster bip_ck_cluster;

create table if not exists test.test_data_all
(
  insert_id        UInt64 COMMENT '插入id unix timestamp nano second',
  object_id String,
  pk UInt64,
  int_value UInt64,
  float_value Float32,
  double_value Float64,
  char_value String,
  varchar_value String,
  time_value DateTime,
  creare_time DateTime default now(),
  flag UInt8 COMMENT '本条记录的状态 0刚插入 1当前状态 2过去状态 3已删除'
)ENGINE Distributed(bip_ck_cluster, 'test', 'test_data', sipHash64(pk));
