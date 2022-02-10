package mysqlx

import (
	"database/sql"
	"sync"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var (
	mysqlPool  = sync.Map{}
	mysqlxPool = sync.Map{}
)

func TakeMySQLConn(dsn string) (*sql.DB, error) {
	client, ok := mysqlPool.Load(dsn)
	if !ok {
		client, e := sql.Open("mysql", dsn)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		mysqlPool.Store(dsn, client)
		return client, nil
	}
	return client.(*sql.DB), nil
}

func TakeMySQLConnx(dsn string) sqlx.SqlConn {
	client, ok := mysqlxPool.Load(dsn)
	if !ok {
		client := sqlx.NewMysql(dsn)
		mysqlxPool.Store(dsn, client)
		return client
	}
	return client.(sqlx.SqlConn)
}
