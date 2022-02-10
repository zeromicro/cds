package choperator

import (
	"time"

	"github.com/zeromicro/cds/cmd/dm/util"
	"github.com/zeromicro/cds/pkg/ckgroup"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	CkGroupOperator struct {
		ckGroup ckgroup.DBGroup
	}
	ClickHouseDescType struct {
		Name              string `db:"name"`
		Type              string `db:"type"`
		DefaultType       string `db:"default_type"`
		DefaultExpression string `db:"default_expression"`
		Comment           string `db:"comment"`
		CodecExpression   string `db:"codec_expression"`
		TTLExpression     string `db:"ttl_expression"`
	}
)

func (cgo *CkGroupOperator) MysqlBatchInsert(insertData [][]interface{}, insertQuery string, arr []util.DataType, indexOfFlag, indexOfInsertID, indexOfPrimKey int) error {
	data := make([][]interface{}, len(insertData))
	for k, v := range insertData {
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
				logx.Error(err)
				return err
			}
			tempData = append(tempData, inter)
		}
		data[k] = tempData
	}
	if err := cgo.ckGroup.ExecAuto(insertQuery, indexOfPrimKey, data); err != nil {
		logx.Error(err)
		return err
	}
	return nil
}

func (cgo *CkGroupOperator) ObtainClickHouseKV(targetDB, targetTable string) (map[string]string, error) {
	var descType []*ClickHouseDescType
	err := cgo.ckGroup.GetQueryNode().QueryRows(&descType, "Desc "+targetDB+"."+targetTable)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	name2Type := make(map[string]string)
	for _, v := range descType {
		name2Type[v.Name] = v.Type
	}
	return name2Type, nil
}

func (cgo *CkGroupOperator) BatchInsert(insertData [][]interface{}, insertQuery string, indexOfPrimKey int) error {
	if len(insertData) == 0 {
		return nil
	}
	if err := cgo.ckGroup.ExecAuto(insertQuery, indexOfPrimKey, insertData); err != nil {
		logx.Error(err)
		return err
	}
	return nil
}
