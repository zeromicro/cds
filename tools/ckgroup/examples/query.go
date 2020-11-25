package main

import (
	"fmt"
	"github.com/tal-tech/cds/tools/ckgroup"
)

func query(group ckgroup.DBGroup) {
	datas := &[]*user{}
	err := group.GetQueryNode().QueryRows(datas, `select id, real_name, city from user where  city = ?`, "test_city")
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := range *datas {
		fmt.Println((*datas)[i])
	}
}

func querySteram(group ckgroup.DBGroup) {
	userChan := make(chan user, 10)
	err := group.GetQueryNode().QueryStream(userChan, `select id, real_name, city from user where  city = ? limit 200`, "test_city")
	if err != nil {
		fmt.Println(err)
		return
	}
	for item := range userChan {
		fmt.Println(item)
	}
}
