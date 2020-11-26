package dbtool

import (
	"errors"
	"fmt"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

const updateMySQL = "update test.test_data set int_value=?, float_value=?, double_value=?, char_value=?, varchar_value=?, time_value=? where pk=?"
const updateCK = "alter table test.test_data on cluster bip_ck_cluster update int_value=?, float_value=?, double_value=?, char_value=?, varchar_value=?, time_value=? where pk=?"
const updateBenchmarkNum = 100

func (s *DBTestToolSqlConn) Update() ([]*DataInstance, error) {
	var query string
	if s.dbType == dbTypeMySQL {
		query = updateMySQL
	} else if s.dbType == dbTypeCK {
		query = updateCK
	} else {
		return nil, errors.New("not support db type")
	}

	dataSet, err := s.Insert()
	if err != nil {
		return nil, err
	}
	dataSet = UpdateDataSet(dataSet)

	err = s.db.Transact(func(session sqlx.Session) error {
		stmt, err := session.Prepare(query)
		if err != nil {
			return err
		}
		for _, item := range dataSet {
			_, err := stmt.Exec(item.IntValue, item.FloatValue, item.DoubleValue, item.CharValue, item.VarCharValue, item.TimeValue, item.PK)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dataSet, nil
}

func (s *DBTestToolSqlConn) UpdateBenchmark() ([]*DataInstance, error) {
	if s.dbType != dbTypeMySQL {
		return nil, errors.New("not support db type")
	}

	logx.MustSetup(logx.LogConf{Mode: "file", Level: "error", Path: "/tmp/suit/"})

	dataSet, err := s.Insert()
	if err != nil {
		return nil, err
	}
	finalDataSet := dataSet
	for i := 0; i < updateBenchmarkNum; i++ {
		fmt.Printf("[INFO] epoch: %d/%d\n", i+1, updateBenchmarkNum)
		dataSet := UpdateDataSet(dataSet)
		for i, item := range dataSet {
			_, err := s.db.Exec(updateMySQL, item.IntValue, item.FloatValue, item.DoubleValue, item.CharValue, item.VarCharValue, item.TimeValue, item.PK)
			if err != nil {
				fmt.Printf("update error:%v\n", err)
				continue
			} else {
				finalDataSet[i] = item
			}
		}
	}
	return finalDataSet, nil
}
