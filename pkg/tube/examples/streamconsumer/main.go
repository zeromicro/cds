package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/zeromicro/cds/pkg/tube"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "service/hera/base/tube/examples/config.yml", "The configure file")

type subscriberConf struct {
	Brokers      []string `json:"Brokers"`
	Topic        string
	Group        string
	SliceSize    int
	WorkerNum    int `json:",default=32"`
	TimerPeriod  int
	ThrottleSize int `json:",default=209715200"`
}

func handler(s []byte) (interface{}, error) {
	// return s + "-handled", errors.New("fdsfasdfsfdsfds")
	return string(s) + "-handled", nil
}

func main() {
	flag.Parse()
	config := subscriberConf{}
	err := conf.LoadConfig(*configFile, &config)
	if err != nil {
		log.Println(err)
	}

	// ==================================================================================================================
	// 1
	sc := tube.MustNewKfkStreamConsumer(config.Topic, config.Group, config.WorkerNum, config.Brokers)
	if sc == nil {
		return
	}
	defer sc.Close()

	// 2
	stream := sc.Subscribe(context.Background(), handler)

	// 3
	i := 0
	for data := range stream {
		if data, ok := data.(error); ok {
			logx.Error(data)
			continue
		}
		if i += 1; i%100 == 0 {
			fmt.Println(data)
			for err := sc.Commit(); err != nil; err = sc.Commit() {
				logx.Error()
			}
			time.Sleep(time.Millisecond * 300)
			fmt.Println("sleep 300ms")
		}
	}
}
