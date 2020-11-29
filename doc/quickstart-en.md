# Quick start

> This article aims to quickly let developers build the `CDS` platform.

## Start the platform

```bash
git clone https://github.com/tal-tech/cds.git
cd cds
sh ./start.sh
```

After the `start.sh` is executed, check if any container exits. At this point, you can open the console, http://localhost:3414/cds.html,  as shown in the figure:

![image-20201118113750898](cds.png)

## Hello world

Now you can add your first **full synchronization task** on the console.

The operation is divided into two parts: `Source(Mysql/Mongo)`, `Target(clickhouse)`

### Source

1. Click `+` in the upper right corner
2. Enter `DSN` 「You can choose `Mysql` or `Mongo`」:
   - `Mysql`: `root:root@tcp(mysql:3306)/test_mysql`
   - `Mongo`: `mongodb://mongo:27017/test_mongo`
3. Click Connect
4. The default table will automatically appear in the selected table. Such as the default `example_mysql (MySQL)` or `example (MongoDB)`

### Target

1. Click **Target Library**. At this time, `DSN` has been filled in by default

2. Click **Refresh**

3. Select the schema synchronized to Clickhouse (**default**)

4. Switch back to **Data Source**, click **Generate Table Statement**

   > Note: Select the `partition` field as needed, here `PARTITION BY toYYYYMM()` can be deleted, or replaced with `PARTITION BY toYYYYMM(datetime)`. Let's use `datetime` as the `partition` field.

5. Click **Execute SQL**, the pop-up below **execution successful**

6. Click **Add**, and the successful execution will pop up below

![image-20201118114502666](image-20201118114502666.png)


Refresh the page, as shown below, it proves that your `Hello World CDS` is successful:


![image-20201118121334999](image-20201118121334999.png)

At the same time, confirm the data in Clickhouse (be sure to make the data more correct!):

![](image-20201118135156133.png)

## Incremental synchronization

Incremental synchronization in `CDS` depends on two components:

- `CONNECTOR`: Monitor data changes from `Mysql/Mongo`, and synchronize the changes to `Kafka`
- `RTU`: consume `Kafka` messages and insert data into `clickhouse`

Therefore, incremental synchronization requires developers to enable these two successively:

1. Click **CONNECTOR monitor**
2. Click `+` in the upper right corner
3. Enter `DSN`:
   - `Mysql`:  `root:root@tcp(mysql:3306)/test_mysql`
   - `Mongo`:  `mongodb://mongo:27017/test_mongo`
4. Click **Add**
5. Click **RTU incremental synchronization**
6. Click on the upper right corner `+`
7. Enter `Source DSN`, same as above
8. Click **Add**
9. Click **Replay** to start

Refresh the page, as shown below, it proves that your **incremental synchronization task** has been added successfully:

![image-20201118135412565](image-20201118135412565.png)

### Verification

In order to verify whether our newly added incremental synchronization task is running normally: execute the initialization database script again and reinsert 100,000 pieces of data.

```shell
cd docker/init
sh ./init.sh
```

Or use the following way:

```python
python3 -m pip install -r requirement.txt
python3 init_db.py
```

You can initialize MySQL and MongoDB separately:

- MySQL: `test_mysql`.`example_mysql`

- MongoDB: `test_mongo`.`example`

Verify `Mysql` incremental synchronization data:

![image-20201118135503830](image-20201118135503830.png)

> Note: The synchronization method of `MongoDB` is similar to `MySQL`, you can follow the same steps to get familiar with `CDS`.