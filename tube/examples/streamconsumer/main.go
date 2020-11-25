package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"cds/tube"

	"github.com/tal-tech/go-zero/core/conf"
	"github.com/tal-tech/go-zero/core/logx"
)

var configFile = flag.String("f", "service/hera/base/tube/examples/config.yml", "The configure file")

func handler(s []byte) (interface{}, error) {
	//return s + "-handled", errors.New("fdsfasdfsfdsfds")
	return string(s) + "-handled", nil
}

func main() {
	flag.Parse()
	config := tube.SubscriberConf{}
	err := conf.LoadConfig(*configFile, &config)
	if err != nil {
		log.Println(err)
	}

	//==================================================================================================================
	//1
	sc := tube.MustNewKfkStreamConsumer(config.Topic, config.Group, config.WorkerNum, config.Brokers)
	if sc == nil {
		return
	}
	defer sc.Close()

	//2
	stream := sc.Subscribe(context.Background(), handler)

	//3
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
