package choperator

import (
	"database/sql"
	"time"

	"github.com/tal-tech/go-zero/core/logx"

	"github.com/tal-tech/cds/cmd/dm/util"
)

type (
	ChProxyOperator struct {
		chProxy *sql.DB
	}
)

var ShangHaiLocation = time.FixedZone("Asia/Shanghai", int((time.Hour * 8).Seconds()))

func (cpo *ChProxyOperator) BatchInsert(insertData [][]interface{}, insertQuery string, arr []util.DataType, indexOfFlag int, indexOfInsertID int) error {
	tx, err := cpo.chProxy.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return err
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			logx.Error(err)
		}
	}()
	for _, v := range insertData {
		var tempData []interface{}
		for key, val := range v {
			if key == indexOfInsertID || key == indexOfFlag {
				tempData = append(tempData, val)
				continue
			}
			inter, err := util.ParseValueByType(func() interface{} {
				if uar, ok := val.(time.Time); ok {
					return uar.In(ShangHaiLocation).Format("2006-01-02 15:04:05")
				} else if uar, ok := val.([]uint8); ok {
					return string(uar)
				}
				return val
			}(), arr[key])
			if err != nil {
				return err
			}
			tempData = append(tempData, inter)
		}
		_, err := stmt.Exec(tempData...)
		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

func (cpo *ChProxyOperator) ObtainClickHouseKV(targetDB, targetTable string) (map[string]string, error) {
	descSQL := "Desc " + targetDB + "." + targetTable
	rows, err := cpo.chProxy.Query(descSQL)
	if err != nil {
		return nil, err
	}
	name2Type := make(map[string]string)
	for rows.Next() {
		var clDescType ClickHouseDescType
		err := rows.Scan(&clDescType.Name, &clDescType.Type, &clDescType.DefaultType, &clDescType.DefaultExpression, &clDescType.Comment, &clDescType.CodecExpression, &clDescType.TTLExpression)
		if err != nil {
			logx.Error(err)
			continue
		}
		//所有的字段
		name2Type[clDescType.Name] = clDescType.Type
	}
	return name2Type, nil
}
