package main

import (
	"fmt"
	"github.com/tal-tech/cds/tools/ckgroup/config"
)

var (
	ckgroupConfig = config.Config{
		ShardGroups: []config.ShardGroupConfig{
			{ShardNode: "tcp://localhost:9000", ReplicaNodes: []string{"tcp://localhost:9001"}},
			{ShardNode: "tcp://localhost:9002", ReplicaNodes: []string{"tcp://localhost:9003"}},
		}, QueryNode: "tcp://localhost:9000"}
)

type user struct {
	Id       int    `db:"id"`
	RealName string `db:"real_name"`
	City     string `db:"city"`
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
