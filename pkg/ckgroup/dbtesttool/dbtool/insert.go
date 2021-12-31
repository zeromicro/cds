package dbtool

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/stores/sqlx"
)

const (
	baseInsertNum int = 10
	// transactionNum         = 10
	benchmarkInsertNum = 10000
	benchmarkThreadNum = 100
)

const (
	insertMySQL = "insert into test.test_data (pk,int_value,float_value,double_value,char_value,varchar_value,time_value) values(?,?,?,?,?,?,?)"
	insertCK    = "insert into test.test_data_all (pk,int_value,float_value,double_value,char_value,varchar_value,time_value) values(?,?,?,?,?,?,?)"
)

func (s *DBTestToolSqlConn) Insert() ([]*DataInstance, error) {
	var query string

	switch s.dbType {
	case dbTypeMySQL:
		query = insertMySQL
	case dbTypeCK:
		query = insertCK
	default:
		return nil, errors.New("not support db type")
	}

	dataSet := GenerateDataSet(baseInsertNum)

	err := s.db.Transact(func(session sqlx.Session) error {
		stmt, err := session.Prepare(query)
		if err != nil {
			return err
		}
		for _, item := range dataSet {
			_, err := stmt.Exec(item.PK, item.IntValue, item.FloatValue, item.DoubleValue, item.CharValue, item.VarCharValue, item.TimeValue)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return dataSet, err
}

func (s *DBTestToolSqlConn) work(wg *sync.WaitGroup, dataSet []*DataInstance, mutex *sync.Mutex, num *int, sqlStr string) {
	defer wg.Done()
	step := 1000

	for i := 0; i < len(dataSet); i += step {
		startIdx := i
		endIdx := startIdx + step
		if endIdx > len(dataSet) {
			endIdx = len(dataSet)
		}
		items := dataSet[startIdx:endIdx]

		err := s.db.Transact(func(session sqlx.Session) error {
			stmt, err := session.Prepare(sqlStr)
			if err != nil {
				return err
			}
			for _, item := range items {
				_, err := stmt.Exec(item.PK, item.IntValue, item.FloatValue, item.DoubleValue, item.CharValue, item.VarCharValue, item.TimeValue)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			// fmt.Printf("insert error:%v\n", err)
		} else {
			mutex.Lock()
			*num += step
			mutex.Unlock()
		}

	}
}

func (s *DBTestToolSqlConn) InsertBenchmarkMySQL() error {
	logx.MustSetup(logx.LogConf{Mode: "file", Level: "error", Path: "/tmp/suit/"})

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	num := 0
	dataSet := generateDataSetBatch(benchmarkInsertNum, benchmarkThreadNum)
	for i := 0; i < benchmarkThreadNum; i++ {
		wg.Add(1)
		go s.work(wg, dataSet[i], mutex, &num, insertMySQL)
	}
	wg.Wait()
	fmt.Printf("total insert %d of %d rows.\n", num, benchmarkInsertNum*benchmarkThreadNum)
	if num != benchmarkInsertNum*benchmarkThreadNum {
		return errors.New("not insert all rows")
	}
	return nil
}

func (s *DBTestToolSqlConn) InsertBenchmarkCK() error {
	logx.MustSetup(logx.LogConf{Mode: "file", Level: "error", Path: "/tmp/suit/"})

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	num := 0
	dataSet := generateDataSetBatch(benchmarkInsertNum, benchmarkThreadNum)
	for i := 0; i < benchmarkThreadNum; i++ {
		wg.Add(1)
		go s.work(wg, dataSet[i], mutex, &num, insertCK)
	}
	wg.Wait()
	fmt.Printf("total insert %d of %d rows.\n", num, benchmarkInsertNum*benchmarkThreadNum)
	if num != benchmarkInsertNum*benchmarkThreadNum {
		return errors.New("not insert all rows")
	}
	return nil
}
