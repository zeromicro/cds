package model

import (
	"log"
	"strings"
	"time"

	"github.com/tal-tech/cds/pkg/mysqlx"
	"github.com/tal-tech/cds/pkg/strx"
)

type (
	Connector struct {
		ID          int       `db:"id"`
		SourceType  string    `db:"source_type"`
		SourceTable string    `db:"source_table"`
		SourceDB    string    `db:"source_db"`
		SourceId    string    `db:"source_id"`
		CreateTime  time.Time `db:"create_time"`
		UpdateTime  time.Time `db:"update_time"`
	}
	ConnectorModel struct {
		base *mysqlx.MySQLModel
	}
)

func NewConnectorModel(dsn string) *ConnectorModel {
	d := &ConnectorModel{}
	var e error
	d.base, _, e = mysqlx.NewMySQLModel("", dsn, Connector{})
	if e != nil {
		log.Fatal(e)
	}
	return d
}

func (d *ConnectorModel) All() ([]*Connector, error) {
	vs, e := d.base.All()
	if e != nil {
		return nil, e
	}
	return vs.([]*Connector), nil
}

func (d *ConnectorModel) Find(id int) (*Connector, error) {
	v, e := d.base.FindBy("id", id)
	if e != nil {
		return nil, e
	}
	return v.(*Connector), nil
}

func (d *ConnectorModel) FindIn(ids ...int) ([]*Connector, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := []interface{}{}
	for _, id := range ids {
		args = append(args, id)
	}
	sql := "id in (" + strings.Join(strx.SliceRepeat("?", len(ids)), ",") + ")"
	vs, e := d.base.QueryWhere(sql, args...)
	if e != nil {
		return nil, e
	}
	return vs.([]*Connector), nil
}

func (d *ConnectorModel) Insert(v *Connector) (int, error) {
	id, e := d.base.Insert(v)
	if e != nil {
		return 0, e
	}
	return int(id), nil
}

func (d *ConnectorModel) DeleteBySourceId(id string) error {
	_, e := d.base.DeleteWhere("source_id = ?", id)
	return e
}

func (d *ConnectorModel) FindByDb(dbname string, start, end int) ([]*Connector, error) {
	var vs interface{}
	var e error
	if dbname != "all" {
		vs, e = d.base.QueryWhere("source_db = ? limit ?,? ", dbname, start, end)
	} else {
		vs, e = d.base.QueryWhere("1=1 limit ?,? ", start, end)
	}
	if e != nil {
		return nil, e
	}
	return vs.([]*Connector), nil
}

func (d *ConnectorModel) GetCountByDb(db string) (int64, error) {
	var cnt int64
	var err error
	if db == "all" {
		cnt, err = d.base.Count("")
	} else {
		cnt, err = d.base.Count("source_db = ?", db)
	}
	if err != nil {
		return 0, err
	}
	return cnt, nil
}

func (d *ConnectorModel) GetAllDb() ([]string, error) {
	var res []string
	err := d.base.Conn.QueryRowsPartial(&res, "select source_db from "+d.base.TableName+" group by source_db")
	if err != nil {
		return nil, err
	}
	return res, nil
}
