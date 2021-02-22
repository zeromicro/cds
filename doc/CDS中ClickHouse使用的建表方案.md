# CDS中ClickHouse使用的建表方案

CDS数据同步与ClickHouse的建表方案紧密相关，下面介绍了两种建表方案。

## 实时表

起初ClickHouse并未考虑数据更新的问题，在官网中有介绍ClickHouse诞生的历史。

>  https://clickhouse.tech/docs/en/introduction/history/

从中可以看出两点：

- 用于日志分析
- 使用非汇总数据在线计算

也就是说数据导入后不考虑变更，而且想要直接分析源数据。

但是现实中我们有很多有价值的数据在事务型数据库中存储，或者我们需要用到的数据在事务型数据库中。而事务型数据库中存储的是状态型数据(可以发生变化)，**对于ClickHouse而言，数据更新是一个非常困难的操作**。

因为上面提到的需求，更新这个功能在随后还是以mutation的形式加入了。这种mutation形式在官网中：

> https://clickhouse.tech/docs/en/sql-reference/statements/alter/update/

有这样的描述 “this is a heavy operation not designed for frequent use.” 而且不支持更新用于计算主键或分区键的列。

可以看出，直接对数据执行更新操作对ClickHouse来说是一件非常糟糕的事。

这种情况在其他用于大数据处理的数据库中也存在，比如以HDFS为支撑的数据仓库，它同样更多的要求数据是不可变的。

即便提供了更新操作，性能都不佳。解决这个需求一般的方法是用程序定期的对过往的数据进行合并，形成一份最新的数据。这种方法的缺点是不能做到实时更新数据。

`cds` 同步设计目标之一是解决事务型数据库数据实时同步至ClickHouse的问题。

`ClickHouse` 有 `MergeTree` 表引擎，这种引擎的特点就是它会自动在后台合并数据。

在 `MergeTree` 表引擎家族中有一个 `ReplacingMergeTree` 的表引擎，它会在合并数据的时候根据主键删除具有相同主键的重复项。不过官网也指出了它的缺点：

> “Data deduplication occurs only during a merge. Merging occurs in the background at an unknown time, so you can't plan for it. 
> Some of the data may remain unprocessed. Although you can run an unscheduled merge using the OPTIMIZE query, don't count on using it, 
> because the OPTIMIZE query will read and write a large amount of data.
> Thus, ReplacingMergeTree is suitable for clearing out duplicate data in the background in order to save space, 
> but it doesn't guarantee the absence of duplicates.”

没有提到的是，在查询时加上 `final` 关键字就可以得到最终数据，而不用动用 `OPTIMIZE` 这种超重型操作。

final也有缺点，就是会导致 `ClickHouse` 以单线程的方式执行，不过这个方式在新的版本中已经改变了https://github.com/ClickHouse/ClickHouse/pull/10463，开发中的新引擎 `MaterializeMySQL`也使用了同样的方法https://github.com/ClickHouse/ClickHouse/issues/4006。加上如果合理使用prewhere和索引，查询速度还算可以。

利用 `ReplacingMergeTree`的表引擎，我们只需要将数据插入 `ClickHouse` ，数据就可以被自动合并了。

![update](https://gitee.com/zyz01/static/raw/master/public/delete.png)

那么删除的操作呢？**可以新增一个删除的标志列。如果源数据被删除，那么插入一条新的删除标志为真的数据，`ReplacingMergeTree` 合并后会变成这一列，查询时在where中添加过滤条件就好了**。

![delete](https://gitee.com/zyz01/static/raw/master/public/update.png))


cds` 中的 `rtu` 模块已经实现了上述 `update/delete`变更 `insert` 的操作。

![rtu](https://gitee.com/zyz01/static/raw/master/public/rtu.png)

`ReplacingMergeTree` 具体建表方式如下：

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

这里有几个需要非常注意的点：
1. 你需要指定一个版本列用于数据合并时确定最新数据，一般指定成 `update_time` 可以实现上面的功能。

2. 数据的合并发生在同一个集群分片的同一个分区里。也就是说对数据插入有所要求。

   **ClickHouse推荐数据直接插入 clickhosue 集群节点的本地存储表中**，而不是通过分布式表插入。这意味着你需要将数据按主键自行散列好后插入对应集群节点的本地存储表。`packge cds/tools/ckgroup `实现了这个功能。

3. 这种表引擎对 `ORDER BY` 的设定有所要求，它必须是主键，但主键可能并非olap查询的常用维度，会导致查询性能不佳。如果需要很高的查询性能，可以考虑定期将数据导入至普通 `MergeTree` 表中。

4. `ReplacingMergeTree`表引擎合并后会删除旧版本的数据。

这种表引擎给 `cds` 中全量同步和增量同步一起进行时可能出现的重复数据自动去重。

## 历史版本与还原

如果想要查询历史中某段时间几天的数据每天的情况，就需要保存每天所有的数据。如果每天保存一个所有数据的快照的话，将会非常占用存储空间，很不经济。

如果只保存增量和变更数据将会节省很多空间，问题是**如何从一堆增量和变更数据中还原每一天的数据？**

对于clickhouse而言，这种情况下不能使用 `ReplacingMergeTree` 表引擎，在上面提到的第4点`ReplacingMergeTree`表引擎合并后会删除旧版本的数据。

在clickhouse中使用普通 `MergeTree`，利用 `argMax`和 **合理的分区** 方案可以实现版本还原。如：

```sql
-- 查询某一日全部用户中编辑角色的数量
SELECT date
     , uniq(user_id)
FROM (
         SELECT date
              , id                                    user_id
              , argMax(users.role, users.update_time) role_
         FROM (
                  SELECT id
                       , update_time
                       , role
                       , toDate('2020-11-11') date
                  FROM default.user
                  WHERE 
                    create_time < toDateTime(date + INTERVAL 1 DAY)
                    AND update_time < toDateTime(date + INTERVAL 1 DAY)
                  ) users
         GROUP BY date, id
         ) day_snap_shot -- 生成当日快照
WHERE role_ = 'editor' 
GROUP BY date;
```
----

上面介绍了两种建表方案，一种实时的，一种带有所有版本变更的。两种方案各有优劣，根据使用场景选择。这两种方案都不完美。

我们仍然在探索新的方法，希望你也能参与进来，一起建设更好的数据。