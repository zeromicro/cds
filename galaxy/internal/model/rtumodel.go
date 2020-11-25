package model

import (
	"log"
	"strings"
	"time"

	"cds/tools/mysqlx"
	"cds/tools/strx"
)

type (
	Rtu struct {
		ID             int       `db:"id"`
		Name           string    `db:"name" index:""`
		SourceType     string    `db:"source_type"`
		SourceDsn      string    `db:"source_dsn"`
		SourceTable    string    `db:"source_table"`
		SourceDb       string    `db:"source_db"`
		SourceQueryKey string    `db:"source_query_key"`
		SourceTopic    string    `db:"source_topic"`
		TargetType     string    `db:"target_type"`
		TargetShards   string    `db:"target_shards"`
		TargetDB       string    `db:"target_db"`
		TargetChProxy  string    `db:"target_ch_proxy"`
		TargetTable    string    `db:"target_table"`
		CreateTime     time.Time `db:"create_time"`
		UpdateTime     time.Time `db:"update_time"`
		Status         string    `db:"status"`
	}
	RtuModel struct {
		base *mysqlx.MySQLModel
	}
)

func NewRtuModel(dsn string) *RtuModel {
	r := &RtuModel{}
	var e error
	r.base, _, e = mysqlx.NewMySQLModel("", dsn, Rtu{})
	if e != nil {
		log.Fatal(e)
	}
	return r
}

func (r *RtuModel) All() ([]*Rtu, error) {
	vs, e := r.base.All()
	if e != nil {
		return nil, e
	}
	return vs.([]*Rtu), nil
}

func (r *RtuModel) Find(id int) (*Rtu, error) {
	v, e := r.base.FindBy("id", id)
	if e != nil {
		return nil, e
	}
	return v.(*Rtu), nil
}

func (r *RtuModel) FindIn(ids ...int) ([]*Rtu, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := []interface{}{}
	for _, id := range ids {
		args = append(args, id)
	}
	sql := "id in (" + strings.Join(strx.SliceRepeat("?", len(ids)), ",") + ")"
	vs, e := r.base.QueryWhere(sql, args...)
	if e != nil {
		return nil, e
	}
	return vs.([]*Rtu), nil
}

func (r *RtuModel) FindByDb(db string, page, pageSize int) ([]*Rtu, error) {
	var vs interface{}
	var e error
	if db != "all" {
		vs, e = r.base.QueryWhere("source_db = ? order by id asc limit ? offset ?", db, pageSize, (page-1)*pageSize)
	} else {
		vs, e = r.base.QueryWhere("1=1 order by id asc limit ? offset ?", pageSize, (page-1)*pageSize)
	}
	if e != nil {
		return nil, e
	}
	return vs.([]*Rtu), nil
}

func (r *RtuModel) GetCountByDb(db string) (int64, error) {
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

func (r *RtuModel) GetAllDb() ([]*Rtu, error) {
	//result, err := r.base.QueryWhere("1 = 1 group by source_db")
	var result []*Rtu
	err := r.base.Conn.QueryRowsPartial(&result, "select source_db from "+r.base.TableName+" group by source_db")
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *RtuModel) Insert(v *Rtu) (int, error) {
	id, e := r.base.Insert(v)
	if e != nil {
		return 0, e
	}
	return int(id), nil
}

func (r *RtuModel) Delete(id int) error {
	_, e := r.base.Delete(id)
	return e
}

func (r *RtuModel) Update(id int, status string) error {
	_, e := r.base.Update(id, "status=?", status)
	return e
}

func (r *RtuModel) GetExist() ([]*Rtu, error) {
	result, err := r.base.QueryWhere("status != ?", "stop")
	if err != nil {
		return nil, err
	}
	return result.([]*Rtu), nil
}
