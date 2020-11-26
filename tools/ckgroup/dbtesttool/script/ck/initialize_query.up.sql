create database if not exists test;

create table if not exists test.test_data_all
(
  pk UInt64,
  int_value UInt64,
  float_value Float32,
  double_value Float64,
  char_value String,
  varchar_value String,
  time_value DateTime,
  creare_time DateTime default now()
)ENGINE Distributed(bip_ck_cluster, 'test', 'test_data', sipHash64(pk));