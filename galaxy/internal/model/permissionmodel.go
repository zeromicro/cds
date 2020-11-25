package model

import (
	"log"
	"time"

	"github.com/tal-tech/cds/tools/mysqlx"
)

type (
	Permission struct {
		ID         int       `db:"id"`
		Module     int       `db:"module" index:""`
		CreateTime time.Time `db:"create_time"`
		UpdateTime time.Time `db:"update_time"`
	}
	PermissionModel struct {
		base *mysqlx.MySQLModel
	}
)

func NewPermissionModel(dsn string) *PermissionModel {
	p := &PermissionModel{}
	var e error
	p.base, _, e = mysqlx.NewMySQLModel("", dsn, Permission{})
	if e != nil {
		log.Fatal(e)
	}
	return p
}
