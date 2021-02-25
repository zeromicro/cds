package handle

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/tal-tech/cds/cmd/rtu/model"
	util "github.com/tal-tech/cds/cmd/rtu/utils"

	"github.com/tal-tech/go-zero/core/logx"
)

var (
//json = jsoniter.ConfigCompatibleWithStandardLibrary
)

type (
	parseEngine struct {
		Category  string
		parseChan chan []*parseStruct

		manager *runEngine

		wg *sync.WaitGroup

		newKeyCached map[string]interface{}
	}

	parseStruct struct {
		Content []byte
		Cmd     int
	}
)

func newParseEngine(category string, manager *runEngine) *parseEngine {
	return &parseEngine{
		Category:  category,
		parseChan: make(chan []*parseStruct),
		wg:        new(sync.WaitGroup),
		manager:   manager,

		newKeyCached: make(map[string]interface{}),
	}
}

func (pe *parseEngine) start() {
	defer pe.wg.Done()
	defer util.Recover(pe.manager.doStop)

LOOP:
	for toParseObjs := range pe.parseChan {
		if len(toParseObjs) == 0 {
			logx.Error("should not be empty")
			continue
		}

		tmpObjs := make([]model.DataInterface, 0, len(toParseObjs))
		for _, toParseObj := range toParseObjs {
			objs := pe.jsonToObj(toParseObj.Content)
			if objs == nil {
				logx.Info(string(toParseObj.Content))
				continue
			}
			for _, obj := range objs {
				err := pe.parseToMap(obj)
				if err != nil {
					continue
				}

				pe.checkAndAddNewColumnKeysToCached(*obj.GetCacheMap())

				tmpObjs = append(tmpObjs, obj)
			}
		}
		if len(tmpObjs) == 0 {
			// todo log
			pe.manager.input.commitChan <- struct{}{}
			continue LOOP
		}

		if ok := pe.doAddNewColumn(); !ok {
			// exit job
			return
		}

		tmpContainers := make([]model.DataInterface, 0, len(tmpObjs))
		for _, obj := range tmpObjs {
			err := pe.formatAndSetValues(obj)
			if err != nil {
				logx.Error(err)
				continue
			}

			tmpContainers = append(tmpContainers, obj)
		}

		pe.manager.insert.insertCh <- tmpContainers
	}
	logx.Info("inputCh is closed, exit now")
}

func (pe *parseEngine) stop() {
	close(pe.parseChan)
	logx.Info("wait now")
	pe.wg.Wait()
}

func (pe *parseEngine) jsonToObj(b []byte) []model.DataInterface {
	var obj model.DataInterface
	switch pe.Category {
	case model.CANALMYSQL:
		obj = &model.CanalMysql{}
	case model.CONNMONGO:
		obj = &model.ConnectorMongo{}
	case model.DBZUMMONGO:
		obj = &model.DebeziumMongo{}
	case model.DBZUMMYSQL:
		obj = &model.DebeziumMySQL{}
	default:
		logx.Errorf("不支持的类型 in jobID: [%v]. topic: [%v]", pe.manager.conf.ID, pe.manager.conf.Kafka.Topic)
		return nil
	}

	err := obj.UnmarshalFromByte(b, pe.manager.mapPool)
	if err != nil {
		logx.Error(err)
		logx.Error(string(b))
		return nil
	}
	objs := obj.Unpack()

	return objs
}

// 把数据格式化到 []interface{}
func (pe *parseEngine) formatAndSetValues(d model.DataInterface) error {
	table := pe.manager.clickhouseTable

	tmp := make([]interface{}, 0, len(table.Columns))
	if _, ok := (*d.GetCacheMap())[table.PrimaryKey]; !ok {
		logx.Errorf("primarikey: %s is required, but not found", table.PrimaryKey)
		return ErrPrimarykeyMiss
	}

	existsKeys := make([]int8, len(table.Columns))
	for index, k := range table.Columns {
		if k == "insert_id" {
			tmp = append(tmp, getInsertID())
			existsKeys[index] = 1
		} else {
			if val, ok := (*d.GetCacheMap())[k]; !ok {
				tmp = append(tmp, table.ColumnsDefaultValue[index])
			} else {
				tmp = append(tmp, val)
				existsKeys[index] = 1
			}
		}
	}
	pe.manager.mapPool.Put(*d.GetCacheMap())
	d.SetCacheMap(nil)
	d.SetValues(tmp)
	d.SetExistsKeys(existsKeys)

	return nil
}

// 判断 key 是否在表的列里面，如果不在，则加入map cached起来
func (pe *parseEngine) checkAndAddNewColumnKeysToCached(keys map[string]interface{}) {
	for k, v := range keys {
		_, ok := pe.manager.clickhouseTable.Types[k]
		_, ok1 := pe.newKeyCached[k]
		if !ok && !ok1 {
			pe.newKeyCached[k] = v
		}
	}
}

// 添加列
func (pe *parseEngine) addNewColumn() error {

	if len(pe.newKeyCached) == 0 {
		return nil
	}
	defer func() {
		pe.newKeyCached = make(map[string]interface{})
	}()
	buf := new(bytes.Buffer)
	Tpl := "add column if not exists `%s` %s"
	cnt := 0
	for k, v := range pe.newKeyCached {
		t := ""
		switch v.(type) {
		case float64, float32:
			t = "Float64"
		case string:
			t = "String"
		case time.Time, *time.Time:
			t = "DateTime"
		case int, int32, int64:
			t = "Int64"
		case map[string]interface{}:
			t = "String"
		default:
			logx.Errorf("[%s] table add column key: [%s] value type [%t] failed", pe.manager.clickhouseTable.Table, k, v)
			continue
		}
		if cnt > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf(Tpl, k, t))
		cnt++
	}

	// 为数据表和视图表增加字段
	for _, t := range []string{"", "_all"} {
		retryCnt := 0
	retry:
		head := fmt.Sprintf("alter table %s.%s%s ", pe.manager.clickhouseTable.DbName, pe.manager.clickhouseTable.Table, t)
		err := pe.manager.chInsertNode.ExecAll(head+buf.String(), nil)
		if err != nil {
			retryCnt++
			logx.Error(err)
			if retryCnt > 3 {
				return err
			}
			time.Sleep(time.Second * time.Duration(cnt*2))
			goto retry
		}
		// 更新查询节点
		if t != "" {
			err := pe.manager.chInsertNode.GetQueryNode().Exec(head+buf.String(), nil)
			if err != nil {
				retryCnt++
				logx.Error(err)
				if retryCnt > 3 {
					return err
				}
				time.Sleep(time.Second * time.Duration(cnt*2))
				goto retry
			}
		}
	}

	return nil
}

// 失败会退出
func (pe *parseEngine) doAddNewColumn() bool {
	if len(pe.newKeyCached) != 0 {
		err := pe.addNewColumn()
		if err != nil {
			logx.Error(err)

			pe.manager.doStop()
			return false
		}
		err = pe.manager.refreshClickhouseTable()
		if err != nil {
			logx.Error(err)
			pe.manager.doStop()
			return false
		}
	}
	return true
}

// note not use
func (pe *parseEngine) merge(objs []model.DataInterface, pkIndex int) []model.DataInterface {
	m := make(map[interface{}]model.DataInterface, len(objs))
	for _, obj := range objs {
		key := obj.GetValues()[pkIndex]
		if v, ok := m[key]; ok {
			oldExistsKey := v.GetExistsKeys()
			oldVals := v.GetValues()
			for index, val := range obj.GetExistsKeys() {
				if val == 1 {
					oldExistsKey[index] = 1
					oldVals[index] = obj.GetValues()[index]
				}
			}
			v.SetValues(oldVals)
			if (v.GetOp() == "c" || v.GetOp() == "r") && obj.GetOp() == "u" {
				continue
			}
			if obj.GetOp() == "d" {
				v.SetValues(obj.GetValues())
				v.SetExistsKeys(obj.GetExistsKeys())
				v.SetOp("d")
				continue
			}
			v.SetOp(obj.GetOp())
		} else {
			m[key] = obj
		}
	}
	result := make([]model.DataInterface, 0, len(m))
	for _, obj := range objs {
		key := obj.GetValues()[pkIndex]

		if v, ok := m[key]; ok {
			result = append(result, v)
			delete(m, key)
		}
	}
	return result
}

func (pe *parseEngine) parseToMap(d model.DataInterface) error {
	table := pe.manager.clickhouseTable
	targetKeys, err := d.ParseToMap(table)
	if err != nil {
		logx.Error(err)
		return err
	}
	d.SetCacheMap(&targetKeys)
	return nil
}
