# 快速开始


## 启动

```bash
git clone https://github.com/tal-tech/cds.git
cd cds
sh ./start.sh
```

当 start.sh 脚本执行结束后，检查是否有 container 出现退出。此时可以打开控制台，http://localhost:3414/cds.html ，如图：

![image-20201118113750898](cds.png)

此时就可以在控制台上添加你的第一个 **全量同步任务**。

操作分为两大部分：`Source(Mysql/Mongo)`，`Target(clickhouse)`

### Source

1. 点击右上角 "+"
2. 输入 `DSN`「可以选择 `Mysql` 或者 `Mongo`」：
   - `Mysql`：`root:root@tcp(mysql:3306)/test_mysql`
   - `Mongo`：`mongodb://mongo:27017/test_mongo`
3. 点击连接
4. 选择表中会自动出现默认表。如默认的 `example_mysql (MySQL)` 或 `example (MongoDB)`

### Target

1. 点击 **目标库**。此时 `DSN` 已经默认填写了

2. 点击 **刷新**

3. 选择同步至 Clickhouse 的 schema（**default**）

4. 切换回 **数据源**，点击 **生成建表语句**

   > 注意：根据需要选择 `partition` 字段，这里 `PARTITION BY toYYYYMM()` 可删除，或 替换成 `PARTITION BY toYYYYMM(datetime)`。我们这以时间做 `partition` 字段。

5. 点击 **执行SQL** ，下方弹出 **执行成功**

6. 点击 **添加**，下方弹出执行成功

![image-20201118114502666](image-20201118114502666.png)


刷新页面，如下图


![image-20201118121334999](image-20201118121334999.png)

同时在 Clickhouse 中确认数据（一定要做数据较对！）：

![](image-20201118135156133.png)

## 实时增量同步

增量同步需要先后开启这两个：
- `CONNECTOR`：从 `Mysql/Mongo` 监听数据变化，并把变化同步到 `Kafka`
- `RTU`：消费 `Kafka` 消息，并插入数据到 `clickhouse`

步骤：
1. 点击 **CONNECTOR监听**
2. 点击右上角 + 
3. 输入 `DSN`：
   - `Mysql`：`root:root@tcp(mysql:3306)/test_mysql`
   - `Mongo`：`mongodb://mongo:27017/test_mongo`
4. 点击 **添加**
5. 点击 **RTU增量同步**
6. 点击右上角 +
7. 输入 `Source DSN`，同上
8. 点击 **添加**
9. 点击 **重放** 使之启动

刷新页面，如下图

![image-20201118135412565](image-20201118135412565.png)

### 验证

再次执行初始化数据库脚本，重新插入100000条数据。

```shell
cd docker/init
sh ./init.sh
```

或者使用以下方式：

```python
python3 -m pip install -r requirement.txt
python3 init_db.py
```

- MySQL: `test_mysql`.`example_mysql`

验证mysql-增量同步数据：

![image-20201118135503830](image-20201118135503830.png)

> 注：`MongoDB` 同步使用方式类似 `MySQL`，大家可以按照同样的步骤熟悉 `CDS` 。
