# CkGroup

> 基于 [clickhouse-go](github.com/ClickHouse/clickhouse-go) 的封装 ，提供更加友好和方便的 API 供开发者使用。

## 特性

* 纯 `Golang` 开发
* 插入时自动做了集群hash分片，避免 `clickhouse` 内分片导致的插入性能损耗和数据分布不均
* 插入失败重试机制 
* 查询结果自动转为 struct

## 安装

- `Go 1.13` 及以上，支持Go的3个最新版本。
- `clichouse (19.16+)`

```go
$ go get -u github.com/tal-tech/cds/tools/ckgroup
```

## 快速体验

确保已安装 `docker` , `docker-compose` 

```shell
./demo.sh
```

运行效果 : 

![demo.gif](demo.gif)

## 使用

`ckgroup` 是对 `clickhouse-go` 的封装。在使用上开发者只需要导入 `ckgroup`，就可以操作 `clickhouse`。

在本例中，我们准备了两条语句：

1. 一条用于插入元组(行)
2. 另一条用于查询。

```go
import "github.com/tal-tech/cds/tools/ckgroup"

// Fill the config
var (
	ckgroupConfig = config.Config{
        ShardGroups: []config.ShardGroupConfig{
            {ShardNode: "tcp://localhost:9000", ReplicaNodes: []string{"tcp://localhost:9001"}},
            {ShardNode: "tcp://localhost:9002", ReplicaNodes: []string{"tcp://localhost:9003"}},
        },
    }
)

func main()  {
    group := ckgroup.MustCKGroup(c)

    // Ready data
    var args [][]interface{}
    for _, item := range generateUsers() {
        args = append(args, []interface{}{item.Id, item.RealName, item.City})
    }

  	// Batch insert, ckgroup will help you to make the shard
    err := group.ExecAuto(`insert into user (id,real_name,city) values (?,?,?)`, 0, args)
    if err != nil {
        panic(err)	// Just for example purpose
    }
  
  	// Query multi rows of the user in Shanghai
  	datas := make([]user, 0)
    err := group.QueryRows(&datas, `select id,real_name,city from user where city=?`, "上海")
    if err != nil {
        panic(err)
    }
    for i := range datas {
        fmt.Println(datas[i])
    }
}

func generateUsers() []user {
    var users []user
    for i := 0; i < 10000; i++ {
        item := user{
            Id:       i,
            RealName: fmt.Sprint("real_name_", i),
            City:     "test_city",
        }
        users = append(users, item)
    }
    return users
}
```

**Feel free to contribute your own examples!**

## TODO

- [x] 改为接口实现 , 方便 test mock
- [ ] 改进 Insert 的易用性
- [ ] 流式查询
- [ ] 。。。
