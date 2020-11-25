#!/bin/python3
# coding: utf8
import pymysql
import pymongo
import random
import string
import datetime


def init_mysql():
    conn = pymysql.connect(
            host="localhost",
            user="root",
            password="root",
            port=3307,
            )
    cur = conn.cursor()
    cur.execute("create database if not exists galaxy")
    cur.execute("create database if not exists test_mysql")
    cur.execute("""
    CREATE TABLE if not exists `test_mysql`.`example_mysql` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `float` DOUBLE NULL,
  `varchar` VARCHAR(45) NULL,
  `datetime` TIMESTAMP NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`));
            """)
    cur.execute("commit")
    cur.execute("truncate `test_mysql`.`example_mysql`")
    cur.execute("commit")
    print("mysql `test_mysql`.`example_mysql` created")

    result = []
    insert_sql = "insert into `test_mysql`.`example_mysql` (`float`, `varchar`, datetime) values (%s, %s, %s)"
    for i in range(1, 100001):
        result.append(
                (
                    random.random(), 
                    "".join(random.choices(string.ascii_letters+string.digits, k=30)),
                    datetime.datetime.now()
                    )
                )
        if len(result) % 10000 == 0:
            print("mysql `test_mysql`.`example` inserted 10000 lines")

            cur.executemany(insert_sql, result)
            result = []

    cur.execute("commit")
    print("mysql `test_mysql`.`example` insert end")

def init_mongo():
    cli = pymongo.MongoClient(host="localhost")
    db = cli["test_mongo"]
    collection = db["example"]
    result = []

    for i in range(1, 100001):
        result.append(
                {
                    "float": random.random(), 
                    "string":"".join(random.choices(string.ascii_letters+string.digits, k=30)),
                    "dt": datetime.datetime.now()
                    }
                )
        if len(result) % 10000 == 0:
            print("mongodb `test_mongo`.`example` inserted 10000 lines")

            collection.insert_many(result)
            result = []

if __name__ == "__main__":
    init_mysql()
    init_mongo()
    
