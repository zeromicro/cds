package main

import (
	"fmt"
	"github.com/tal-tech/cds/tools/ckgroup"
	"time"
)

func main() {
	group := ckgroup.MustCKGroup(ckgroupConfig, ckgroup.WithRetryNum(3))
	//queryRowNoType(group)
	//queryRowsNoType(group)
	//query(group)
	//querySteram(group)
	demo(group)
}

func demo(group ckgroup.DBGroup) {
	fmt.Println(`clickhouse 建立 user 表
--------------------------------------------------
create table user on cluster bip_ck_cluster
(
    id        Int64,
    real_name String,
    city      String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/user',
           '{replica}') partition by (city) order by id`)
	fmt.Println()
	createTable(group)
	fmt.Printf("使用 ckgroup 向两个节点的 user 表共插入一万条数据\n\n")
	insert2(group)
	fmt.Printf("------------------插入完成---------------------\n\n")
	fmt.Printf("在每个节点运行 uniqExact(id) 结果如下 :\n\n")
	time.Sleep(time.Second * 2)
	pirntCount(group)
}

func createTable(group ckgroup.DBGroup) {
	err := group.GetQueryNode().Exec(`create table if not exists user on cluster bip_ck_cluster
(
    id        Int64,
    real_name String,
    city      String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/user',
           '{replica}') partition by (city) order by id`)
	if err != nil {
		panic(err)
	}
}

func pirntCount(group ckgroup.DBGroup) {
	num := 1
	for _, conn := range group.GetAllNodes() {
		data := struct {
			Count int `db:"cnt"`
		}{}
		err := conn.QueryRow(&data, `select uniqExact(id) cnt from user`)
		if err == nil {
			fmt.Printf("%d 号节点 cnt: %d\n", num, data.Count)
		} else {
			fmt.Println("query count error:", err.Error())
		}
		num++
	}
}
