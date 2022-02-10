package clickhousex

import (
	"database/sql"
	"sync"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/clickhouse"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var (
	clientPool  = sync.Map{}
	clientxPool = sync.Map{}
)

func TakeClient(dsn string) (*sql.DB, error) {
	client, ok := clientPool.Load(dsn)
	if !ok {
		client, e := sql.Open("clickhouse", dsn)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		clientPool.Store(dsn, client)
		return client, nil
	}
	return client.(*sql.DB), nil
}

func TakeClientx(dsn string) sqlx.SqlConn {
	c, ok := clientxPool.Load(dsn)
	if !ok {
		c := clickhouse.New(dsn)
		clientxPool.Store(dsn, c)
		return c
	}
	return c.(sqlx.SqlConn)
}
