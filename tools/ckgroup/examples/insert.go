package main

import (
	"cds/tools/ckgroup"
	"fmt"
)

func insert(group ckgroup.DBGroup) {
	var args [][]interface{}
	for _, item := range generateUsers() {
		args = append(args, []interface{}{item.Id, item.RealName, item.City})
	}

	err := group.ExecAuto(`insert into user (id,real_name,city) values (?,?,?)`, 0, args)
	if err != nil {
		fmt.Println(err)
	}
}

func insert2(group ckgroup.DBGroup) {
	users := generateUsers()
	err := group.InsertAuto(`insert into user (id,real_name,city) values (#{id},#{real_name},#{city})`, `id`, users)
	if err != nil {
		fmt.Println(err)
	}
}
