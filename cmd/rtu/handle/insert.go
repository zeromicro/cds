package handle

import (
	"errors"
	"sync"
	"time"

	"github.com/zeromicro/cds/cmd/rtu/model"
	util "github.com/zeromicro/cds/cmd/rtu/utils"
	"github.com/zeromicro/go-zero/core/logx"
)

// ErrPrimarykeyMiss 主键不存在
var ErrPrimarykeyMiss = errors.New("primarikey is required, but not found")

const (
	maxErrCnt = 30
)

type insertEngine struct {
	insertCh chan []model.DataInterface

	wg *sync.WaitGroup

	manager *runEngine

	insertOkChan chan int8 // 标记插入成功
	exitChan     chan struct{}
}

func newInsertEngine(manager *runEngine) *insertEngine {
	eg := &insertEngine{
		insertCh: make(chan []model.DataInterface, 1),
		wg:       new(sync.WaitGroup),

		manager: manager,

		insertOkChan: make(chan int8, 1),
		exitChan:     make(chan struct{}),
	}
	return eg
}

func (ie *insertEngine) start() {
	defer ie.wg.Done()
	defer util.Recover(ie.manager.doStop)
	defer func() {
		logx.Infof("[%s] insert exit", ie.manager.conf.Source.Topic)
	}()

	for {
		select {
		case _, ok := <-ie.manager.closeChan:
			if !ok {
				return
			}
		case objs, ok := <-ie.insertCh:
			if !ok {
				logx.Infof("[%s] insertCh is closed, exit now", ie.manager.conf.Source.Topic)
				return
			}
			// 插入数量的计数器
			toInsert := make([]interface{}, 0, len(objs))
			for _, obj := range objs {
				if obj == nil {
					continue
				}

				if obj == nil || obj.GetValues() == nil {
					continue
				}
				toInsert = append(toInsert, obj.GetValues())
			}
			// 如果大于0则flush一下
			if len(toInsert) > 0 {
				ie.insert(toInsert)
			} else {
				logx.Error("shoud not be empty, block here")
			}

			select {
			case <-ie.manager.closeChan:
				return
			case _, ok := <-ie.insertOkChan:
				logx.Infof("[%s] insert ok", ie.manager.conf.Source.Topic)
				if !ok {
					return
				}
				ie.manager.input.commitChan <- struct{}{}
			case _, ok := <-ie.exitChan:
				if !ok {
					return
				}
			}
		}
	}
}

func (ie *insertEngine) stop() {
	close(ie.insertCh)
	close(ie.exitChan)
	ie.wg.Wait()
}

func (ie *insertEngine) insert(items []interface{}) {
	defer func() {
		logx.Infof("[%s] insert func exit", ie.manager.conf.Source.Topic)
	}()
	res := make([][]interface{}, 0, len(items))

	for _, i := range items {
		v, ok := i.([]interface{})
		if !ok {
			continue
		}

		res = append(res, v)
	}
	if len(res) == 0 {
		ie.manager.monitorVec.db.Inc(ie.manager.labels.insertOkLabel)
		// ie.insertOkChan <- 1

		logx.Errorf("[%s] shoud not be empty, block here", ie.manager.conf.Source.Topic)
		//  will block
		return
	}
	errCnt := 1
retry:
	if *ie.manager.isClosed >= 1 {
		return
	}
	if errCnt > 1 {
		logx.Errorf("[%s] retry [%d]", ie.manager.conf.Source.Topic, errCnt)
	}
	err := ie.manager.chInsertNode.ExecAuto(
		ie.manager.clickhouseTable.InsertSQL,
		ie.manager.clickhouseTable.PrimaryKeyIndex,
		res,
	)
	if err != nil {
		ie.manager.monitorVec.db.Inc(ie.manager.labels.insertFailedLabel)

		sleepDuration := min(errCnt, maxErrCnt*2)
		logx.Errorf("[%s] err: [%s], will sleep %d", ie.manager.conf.Source.Topic, err, sleepDuration)
		time.Sleep(time.Second * time.Duration(sleepDuration))
		if errCnt < maxErrCnt*2 {
			errCnt <<= 1
		}
		goto retry
	}

	ie.manager.monitorVec.db.Inc(ie.manager.labels.insertOkLabel)
	ie.insertOkChan <- 1
}

func getInsertID() int64 {
	return time.Now().UnixNano()
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
