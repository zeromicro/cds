package model

import (
	"log"
	"strings"
	"time"

	"github.com/tal-tech/cds/pkg/mysqlx"
	"github.com/tal-tech/cds/pkg/strx"
)

type (
	Dm struct {
		ID              int       `db:"id"`
		Name            string    `db:"name" length:"24" index:""`
		SourceType      string    `db:"source_type"`
		SourceDsn       string    `db:"source_dsn"`
		SourceDb        string    `db:"source_db"`
		SourceTable     string    `db:"source_table"`
		SourceQueryKey  string    `db:"source_query_key"`
		TargetType      string    `db:"target_type"`
		TargetShards    string    `db:"target_shards"`
		TargetDB        string    `db:"target_db"`
		TargetChProxy   string    `db:"target_ch_proxy"`
		TargetTable     string    `db:"target_table"`
		WindowStartHour int       `db:"window_start_hour"`
		WindowEndHour   int       `db:"window_end_hour"`
		CreateTime      time.Time `db:"create_time"`
		UpdateTime      time.Time `db:"update_time"`
		Suffix          string    `db:"suffix"`
	}
	DmModel struct {
		base *mysqlx.MySQLModel
	}
)

func NewDmModel(dsn string) *DmModel {
	d := &DmModel{}
	var e error
	d.base, _, e = mysqlx.NewMySQLModel("", dsn, Dm{})
	if e != nil {
		log.Fatal(e)
	}
	return d
}

func (d *DmModel) All() ([]*Dm, error) {
	vs, e := d.base.All()
	if e != nil {
		return nil, e
	}
	return vs.([]*Dm), nil
}

func (d *DmModel) Find(id int) (*Dm, error) {
	v, e := d.base.FindBy("id", id)
	if e != nil {
		return nil, e
	}
	return v.(*Dm), nil
}

func (d *DmModel) FindIn(ids ...int) ([]*Dm, error) {
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
	return vs.([]*Dm), nil
}

func (d *DmModel) Insert(v *Dm) (int, error) {
	id, e := d.base.Insert(v)
	if e != nil {
		return 0, e
	}
	return int(id), nil
}

func (d *DmModel) Delete(id int) error {
	_, e := d.base.Delete(id)
	return e
}

func (r *DmModel) FindByDb(db string, page, pageSize int) ([]*Dm, error) {
	var vs interface{}
	var e error
	if db != "all" {
		vs, e = r.base.QueryWhere("target_db = ? order by id asc limit ? offset ?", db, pageSize, (page-1)*pageSize)
	} else {
		vs, e = r.base.QueryWhere("1=1 order by id asc limit ? offset ?", pageSize, (page-1)*pageSize)
	}
	if e != nil {
		return nil, e
	}
	return vs.([]*Dm), nil
}

func (r *DmModel) GetCountByDb(db string) (int64, error) {
	var cnt int64
	var err error
	if db != "all" {
		cnt, err = r.base.Count("source_db = ?", db)
	} else {
		cnt, err = r.base.Count("")
	}
	if err != nil {
		return 0, err
	}
	return cnt, nil
}

func (r *DmModel) GetAllDb() ([]*Rtu, error) {
	var result []*Rtu
	err := r.base.Conn.QueryRowsPartial(&result, "select source_db from "+r.base.TableName+" group by source_db")
	if err != nil {
		return nil, err
	}

	return result, nil
}
