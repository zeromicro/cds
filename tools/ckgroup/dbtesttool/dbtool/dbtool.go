package dbtool

import (
	"github.com/tal-tech/go-zero/core/stores/mongo"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

type (
	DBTestTool interface {
		Insert() ([]*DataInstance, error)
		Clean() error
		SetUp() error
		Update() ([]*DataInstance, error)
		Delete() ([]*DataInstance, error)
		Select([]*DataInstance) (map[int64]int, error)
	}

	DBTestToolSqlConn struct {
		db      sqlx.SqlConn
		dbType  int
		isQuery bool
	}

	DBTestToolMongo struct {
		db         *mongo.Model
		Database   string
		Collection string
	}
)

const (
	dbTypeMySQL = iota
	dbTypeCK
)

func NewDBTestToolOnMySQL(dataSource string) DBTestTool {
	return &DBTestToolSqlConn{db: sqlx.NewMysql(dataSource), dbType: dbTypeMySQL}
}

func NewDBTestToolOnMongo(dataSource string, collection string) DBTestTool {
	return &DBTestToolMongo{db: mongo.MustNewModel(dataSource, collection)}
}

func NewDBTestToolOnCK(conn sqlx.SqlConn) DBTestTool {
	return &DBTestToolSqlConn{db: conn, dbType: dbTypeCK}
}

func NewDBTestToolOnCKQuery(conn sqlx.SqlConn) DBTestTool {
	return &DBTestToolSqlConn{db: conn, dbType: dbTypeCK, isQuery: true}
}
