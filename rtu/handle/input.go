package handle

import (
	"context"
	"sync"
	"time"

	util "cds/rtu/utils"
	"cds/tube"

	"github.com/tal-tech/go-zero/core/logx"
)

type inputEngine struct {
	kafkaConsumer tube.KfkStreamConsumer
	inputChan     chan interface{}
	commitChan    chan struct{} // 标记完成一轮，可以commit&&fetch

	wg *sync.WaitGroup

	manager *runEngine

	kafkaCancel context.CancelFunc
}

func newInput(manager *runEngine) *inputEngine {
	kfks := tube.MustNewKfkStreamConsumer(
		manager.conf.Kafka.Topic,
		manager.conf.Kafka.Group,
		manager.conf.Kafka.WorkerNum,
		manager.conf.Kafka.Brokers,
	)

	ctx, cancel := context.WithCancel(context.Background())

	streamMsgChan := kfks.Subscribe(ctx, func(bytes []byte) (interface{}, error) {
		return bytes, nil
	})

	return &inputEngine{
		kafkaConsumer: kfks,
		inputChan:     streamMsgChan,
		commitChan:    make(chan struct{}, 1),
		wg:            new(sync.WaitGroup),

		manager: manager,

		kafkaCancel: cancel,
	}
}

func (inpe *inputEngine) commitOrNext(data *[]*parseStruct, size *int) {
	if len(*data) == 0 {
		//logx.Info("commit exit (reason len(data) = 0)")
		return
	}
	inpe.manager.monitorVec.runtime.Set(inpe.manager.labels.batchLengthLable, len(*data))
	inpe.manager.monitorVec.runtime.Set(inpe.manager.labels.batchSizeLable, *size)

	logx.Infof("[%s] data len is %d", inpe.manager.conf.Source.Topic, len(*data))
	inpe.manager.parse.parseChan <- *data
	<-inpe.commitChan
	err := inpe.kafkaConsumer.Commit()
	if err != nil {
		logx.Errorf("[%s] %s", inpe.manager.conf.Source.Topic, err)
	}
	logx.Infof("[%s] commit ok", inpe.manager.conf.Source.Topic)

	*data = (*data)[:0]
	*size = 0
}

func (inpe *inputEngine) start() {
	// TODO in etcd config
	batch := 30000
	maxSize := 30 * 1024 * 1024
	size := 0

	tmp := make([]*parseStruct, 0, batch)

	defer util.Recover(inpe.manager.doStop)
	defer inpe.wg.Done()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			// auto commit
			inpe.commitOrNext(&tmp, &size)
		case Ibyte, ok := <-inpe.inputChan:
			if !ok {
				logx.Infof("[%s]kafka is closed, exit now", inpe.manager.conf.Source.Topic)
				return
			}

			msg, ok := Ibyte.([]byte)
			if !ok || len(msg) == 0 {
				continue
			}
			size += len(msg)

			inpe.manager.monitorVec.kafka.Inc(inpe.manager.labels.msgCntLabel)
			tmp = append(tmp, &parseStruct{Content: msg})
			if len(tmp) >= batch || size >= maxSize {
				inpe.commitOrNext(&tmp, &size)
			}
		}
	}
}

func (inpe *inputEngine) stop() {
	err := inpe.kafkaConsumer.Close()
	if err != nil {
		logx.Error(err)
	}
	logx.Info("kafka closed")

	inpe.kafkaCancel()
	logx.Info("kafka cancel")

	inpe.wg.Wait()
}
