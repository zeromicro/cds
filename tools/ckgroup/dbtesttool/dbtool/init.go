package dbtool

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"runtime"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (s *DBTestToolSqlConn) SetUp() error {
	filePath := s.getPackagePath()
	if s.isQuery {
		filePath += "/script/ck/initialize_query.up.sql"
	} else {
		switch s.dbType {
		case dbTypeMySQL:
			filePath += "/script/mysql/initialize_schema.up.sql"
		case dbTypeCK:
			filePath += "/script/ck/initialize_schema.up.sql"
		default:
			return errors.New("error db type")
		}
	}

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	statements := strings.Split(string(data), ";")
	for _, item := range statements {
		query := strings.TrimSpace(item)
		if len(query) != 0 {
			_, err = s.db.Exec(query)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *DBTestToolSqlConn) Clean() error {
	filePath := s.getPackagePath()
	if s.isQuery {
		filePath += "/script/ck/initialize_query.down.sql"
	} else {
		switch s.dbType {
		case dbTypeMySQL:
			filePath += "/script/mysql/initialize_schema.down.sql"
		case dbTypeCK:
			filePath += "/script/ck/initialize_schema.down.sql"
		default:
			return errors.New("error db type")
		}
	}
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(string(data))
	return err
}

func (s *DBTestToolSqlConn) getPackagePath() string {
	_, filename, _, _ := runtime.Caller(1)
	filePath := strings.TrimSuffix(filename, "/dbtool/init.go")
	return filePath
}
