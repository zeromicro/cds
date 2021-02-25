create database if not exists `test` default character set utf8mb4 collate utf8mb4_unicode_ci;

use test;

drop table if exists `test_data`;
create table `test_data`
(
  pk bigint not null,
  int_value bigint null,
  float_value float null,
  double_value double null,
  char_value char(16) null,
  varchar_value varchar(16) null,
  time_value timestamp not null,
  constraint test_data_pk
    primary key (pk)
);