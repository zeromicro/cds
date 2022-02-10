# CkGroup
[clickhouse-go](github.com/ClickHouse/clickhouse-go) 的封装 

目的是操作 ClickHouse 集群内的多个节点



## 特性

* 插入时在代码层面做 hash , 防止 ClickHouse 的分布式表负担过大
* 查询方便 , 支持普通查询和流式查询 
* 查询结果可以映射为 struct 或 map
* 插入失败时有重试机制


## 例子

具体参考 `examples` 目录下的代码

### insert

```golang
imports....

func main() {
	var c = config.Config{
		ShardGroups: []config.ShardGroupConfig{
			{ShardNode: "tcp://localhost:9000", ReplicaNodes: []string{"tcp://localhost:9001"}},
			{ShardNode: "tcp://localhost:9002", ReplicaNodes: []string{"tcp://localhost:9003"}},
		}}
	
	group := ckgroup.MustCKGroup(c)

    users := generateUsers()
    err := group.InsertAuto(`insert into user (id,real_name,city) values (#{id},#{real_name},#{city})`, `id`, users)
	if err != nil {
		fmt.Println(err)
	}
}

```



### query

```golang
improt....

type user struct {
	Id       int `db:"id"`
	RealName string `db:"real_name"`
	City     string `db:"city"`
}

func main() {
	var c = config.Config{QueryNode: "clickhouse dns url"}
	group := ckgroup.MustCKGroup(c)

	datas := &[]*user{}
	err := group.GetQueryNode().QueryRows(datas, `select id, real_name, city from user where  city = ?`, "上海")
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := range *datas {
		fmt.Println((*datas)[i])
	}
}
```





