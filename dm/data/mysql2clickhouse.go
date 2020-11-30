package data

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tal-tech/cds/dm/choperator"
	"github.com/tal-tech/cds/dm/cmd/sync/config"
	"github.com/tal-tech/cds/dm/util"
	"github.com/tal-tech/go-zero/core/logx"
)

type (
	Mysql2ClickHouse struct {
		mysqlConn  *sql.DB
		chOperator *choperator.ChOperator
		controller *chan bool
		etcdHosts  []string
	}
)

func NewMysql2ClickHouseModel(mysqlConn *sql.DB, chOperator *choperator.ChOperator, controller *chan bool, etcdHosts []string) *Mysql2ClickHouse {
	return &Mysql2ClickHouse{mysqlConn: mysqlConn, chOperator: chOperator, controller: controller, etcdHosts: etcdHosts}
}

func (mc *Mysql2ClickHouse) MysqlInertIntoClickHouse(job *config.Job) (string, error) {
	ckMap, err := (*mc.chOperator).ObtainClickHouseKV(job.Target.DB, job.Target.Table)
	if err != nil {
		logx.Error(err)
		return "", err
	}

	tc := util.NewMysqlTypeConvModel(mc.mysqlConn)
	typeMap, pks := tc.ObtainMysqlTypeMap(job.Source.Table)

	selectPreSQL, sqlForInsert, indexOfFlag, indexOfInsertID, indexOfPrimKey := combineSQL(ckMap, job.Source.Table, job.Target.Table, job.Target.DB, pks, 20000)

	var typeArr []util.DataType
	kt := make([]string, 0, len(ckMap))
	for k := range ckMap {
		kt = append(kt, k)
	}
	sort.Strings(kt)
	for i := 0; i < len(kt); i++ {
		switch kt[i] {
		case "insert_id":
			typeArr = append(typeArr, util.DataTypeInt)
		case "ck_is_delete":
			typeArr = append(typeArr, util.DataTypeInt)
		default:
			typeArr = append(typeArr, typeMap[kt[i]])
		}
	}
	first := true
	pksToSql := make([]string, 0, len(pks))

	pkOrder := make([]int, 0, len(pks))
	indexs := 0
	for pk, order := range indexOfPrimKey {
		pksToSql = append(pksToSql, pk+">?")
		pkOrder = append(pkOrder, order)
		indexs = order
	}
	firstSql := fmt.Sprintf(selectPreSQL, "")
	remainSql := fmt.Sprintf(selectPreSQL, "where "+strings.Join(pksToSql, " and "))
	var insertData [][]interface{}

	for {
		var selectSQL string
		args := make([]interface{}, 0, len(pks))
		if first {
			selectSQL = firstSql
			first = !first
		} else {
			if len(insertData) == 0 {
				break
			}
			selectSQL = remainSql

			last := insertData[len(insertData)-1]
			for _, i := range pkOrder {
				args = append(args, last[i])
			}
			insertData = insertData[:0]
		}
		rows, err := mc.mysqlConn.Query(selectSQL, args...)
		if err != nil {
			logx.Error(err)
			return "", err
		}
		for rows.Next() {
			err := formatToInsert(rows, ckMap, indexOfFlag, indexOfInsertID, &insertData)
			if err != nil {
				return "stopped", nil
			}
		}
		select {
		case <-*mc.controller:
			logx.Info("Task ID:" + job.ID + " has been stopped manually")
			return "stopped", nil
		default:
			err = (*mc.chOperator).MysqlBatchInsert(insertData, sqlForInsert, typeArr, indexOfFlag, indexOfInsertID, indexs)
			if err != nil {
				logx.Error(err)
				return "", err
			}
		}
	}

	return "", nil
}

func formatToInsert(rows *sql.Rows, ckMap map[string]string, indexOfFlag, indexOfInsertID int, insertData *[][]interface{}) error {
	countOfColumn := len(ckMap) - 2 //字段数量为Ck字段数减2 (flag和insert_id)
	temp := make([]interface{}, countOfColumn)
	tempPointer := make([]interface{}, countOfColumn)
	for i := 0; i < countOfColumn; i++ {
		tempPointer[i] = &temp[i]
	}
	err := rows.Scan(tempPointer...)
	if err != nil {
		logx.Error(err)
		return err
	}
	//先填 Flag Insert_ID 进去 然后把Mysql的数据再塞进去
	allData, err := combineData(temp, indexOfFlag, indexOfInsertID)
	if err != nil {
		logx.Error(err)
		return err
	}
	*insertData = append(*insertData, allData)
	return nil
}

//This func create the sql which 1.get data from mysql 2.insert data to clickhouse
func combineSQL(ckTypeMap map[string]string, sourceTable, targetTable, targetDB string, pks map[string]int, batchCnt int) (string, string, int, int, map[string]int) {
	var selectSqlBuilder, insertSqlBuilder strings.Builder
	indexOfFlag, indexOfInertID := -1, -1
	//prepare the query
	selectSqlBuilder.WriteString("SELECT ")
	insertSqlBuilder.WriteString("INSERT INTO " + targetDB + "." + targetTable + " (")
	var suffix string
	var ar, des []string

	kt := make([]string, 0, len(ckTypeMap))
	for k := range ckTypeMap {
		kt = append(kt, k)
	}
	sort.Strings(kt)
	pksIndex := make(map[string]int, len(pks))
	for i := 0; i < len(kt); i++ {
		if _, ok := pks[kt[i]]; ok {
			//pksIndex = append(pksIndex, i)
			pksIndex[kt[i]] = i
		}
		switch {
		case kt[i] != "ck_is_delete" && kt[i] != "insert_id":
			ar = append(ar, kt[i])
		case kt[i] == "ck_is_delete":
			indexOfFlag = i
		case kt[i] == "insert_id":
			indexOfInertID = i
		}
		des = append(des, kt[i])
	}
	for i := 0; i < len(ar); i++ {
		selectSqlBuilder.WriteString("`" + ar[i] + "`")
		if i != len(ar)-1 {
			selectSqlBuilder.WriteString(",")
		}
	}
	for i := 0; i < len(kt); i++ {
		suffix += "?"
		insertSqlBuilder.WriteString(des[i])
		if i != len(kt)-1 {
			insertSqlBuilder.WriteString(",")
			suffix += ","
		}
	}
	selectSqlBuilder.WriteString(" FROM `" + sourceTable + "`")
	selectSqlBuilder.WriteString(" %s limit ")
	selectSqlBuilder.WriteString(strconv.Itoa(batchCnt))
	insertSqlBuilder.WriteString(") VALUES (" + suffix + ")")
	return selectSqlBuilder.String(), insertSqlBuilder.String(), indexOfFlag, indexOfInertID, pksIndex
}

func combineData(data []interface{}, indexOfFlag int, indexOfInsertID int) ([]interface{}, error) {
	if indexOfFlag == -1 || indexOfInsertID == -1 {
		return nil, errors.New("cannot locate the flag or insertID index")
	}
	result := make([]interface{}, len(data)+2)
	result[indexOfFlag] = 0

	intNum, err := strconv.Atoi(strconv.FormatInt(time.Now().UnixNano(), 10))
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	result[indexOfInsertID] = uint64(intNum)
	var i, j int
	//俩指针 一个扫描data 当遇到j=flag | insertID 时，i不动 j++
	for i < len(data) {
		switch {
		case j == indexOfFlag || j == indexOfInsertID:
			j++
		default:
			result[j] = data[i]
			i++
			j++
		}
	}
	return result, nil
}
