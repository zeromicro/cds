package main

import (
	"cds/tools/ckgroup"
	"fmt"
)

func insert2(group ckgroup.DBGroup) {
	users := generateUsers()
	err := group.InsertAuto(`insert into user (id,real_name,city) values (#{id},#{real_name},#{city})`, `id`, users)
	if err != nil {
		fmt.Println(err)
	}
}
