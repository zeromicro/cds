<p align="left">
  <img width ="200px" src="https://github.com/zeromicro/cds/raw/master/doc/logo.png">
</p>

# ClickHouse Data Synchromesh
Data syncing in golang for ClickHouse.


based on [go-zero](https://github.com/tal-tech/go-zero) 

### ARCH

Data workflow of a typical data warehouse architecture 

![avatar](https://github.com/zeromicro/cds/raw/master/doc/clickhouse_arch.png)

### Design of Data Sync

Automatically synchronizing data from MySQL/MongoDB data source to ClickHouse cluster in real time(< 2min).

![同步drawio](https://github.com/zeromicro/cds/raw/master/doc/%E5%90%8C%E6%AD%A5drawio.png)


# start up

```bash
git clone https://github.com/zeromicro/cds.git
cd cds
make up
```

After the build , pay attention to check if any container exits abnormally.

click http://localhost:3414/cds.html to accessing the web interface.


using username and password below to login
```
user: admin@email.com
password: 123456
```
## create table in ClickHouse for syncing MySQL(or MongoDB) data

Chose "create table" tab

```
1. Click "Target ClickHouse Database Info",
2. Click “connect”
3. Select the schema ("default") synchronized to Clickhouse
4. switch to "Data Source" 
5. MySQL input connection string root:root@tcp(mysql:3306)/test_mysql
or
MongoDB input connection string mongodb://mongo1:30001/test_mongo
6. Click “connect”
7. Select the table, such as the default example_mysql (MySQL) or example (MongoDB)
8. click "Generate create Table SQL" 
 Note: select the partition field as needed, here'PARTITION BY toYYYYMM()' can be deleted, or replaced with'PARTITION BY toYYYYMM(dt)'
9. . Click "send SQL to ClickHouse", and the successful execution will pop up below
```


## One-time full data synchronization:
Chose "full sync" tab
```
1. Click "+" in the upper right corner
2. MySQL input connection string root:root@tcp(mysql:3306)/test_mysql
or
MongoDB input connection string mongodb://mongo1:30001/test_mongo
3. Click “connect”
4. Select the table, such as the default example_mysql (MySQL) or example (MongoDB)
5. Click "Target ClickHouse Database Info"
6. Click “connect”
7. Select the schema ("default") synchronized in Clickhouse
10. Click "Add", a pop-up below shows successful execution
```


### check task status
Refresh the page 


### Confirm data in Clickhouse

![image-20201118135156133](doc/image-20201118135156133.png)

## Turn on real-time incremental synchronization

Take mysql as an example

chose "Connector" tab
```
1. Click "+" in the upper right corner
2. MySQL input connection string root:root@tcp(mysql:3306)/test_mysql
3. select table 
4. Click "Add"
```
chose "Incremental Sync" tab

```
1. Click "+" in the upper right corner
2. MySQL input connection string root:root@tcp(mysql:3306)/test_mysql
3. select table 
4. Click "Target ClickHouse Database Info"
5. Click “connect”
6. Select the schema ("default") synchronized in Clickhouse
7. Click "Add", a pop-up below shows successful execution
```
refresh page

### Verify incremental update
Execute the initialization database script again, you can insert 100000 rows of data again.

```
cd sit/docker/
sh ./init.sh
```

Verify the incremental data of mysql in clickhouse：

![image-20201118135503830](doc/image-20201118135503830.png)

## clean up
To clean up all the docker containers started above and restore the initial state, you can ：

```
cd cds
make down
```

only clean 

```
cd cds
make docker_clean
```


### data model in clickhouse
[CDS中ClickHouse使用的建表方案](doc/CDS中ClickHouse使用的建表方案.md)

## help

[提问的智慧](https://github.com/ryanhanwu/How-To-Ask-Questions-The-Smart-Way/blob/main/README-zh_CN.md)

[如何有效的报告bug](https://www.chiark.greenend.org.uk/~sgtatham/bugs-cn.html)

---

if you like this project and want to support it，please `star` 🤝

