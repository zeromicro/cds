package model

import (
	"log"
	"time"

	"github.com/tal-tech/cds/tools/mysqlx"
)

type (
	Group struct {
		ID         int       `db:"id"`
		Name       string    `db:"name" length:"36"`
		CreateTime time.Time `db:"create_time"`
		UpdateTime time.Time `db:"update_time"`
	}
	GroupModel struct {
		base *mysqlx.MySQLModel
	}
)

func NewGroupModel(dsn string) *GroupModel {
	g := &GroupModel{}
	var e error
	g.base, _, e = mysqlx.NewMySQLModel("", dsn, Group{})
	if e != nil {
		log.Fatal(e)
	}
	return g
}
