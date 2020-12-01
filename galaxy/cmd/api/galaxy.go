package main

import (
	"flag"
	"runtime"

	"github.com/robfig/cron"
	"github.com/tal-tech/go-zero/core/conf"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/rest"

	"github.com/tal-tech/cds/galaxy/internal/clients"
	"github.com/tal-tech/cds/galaxy/internal/config"
	"github.com/tal-tech/cds/galaxy/internal/handler"
	"github.com/tal-tech/cds/galaxy/internal/svc"
)

var configFile = flag.String("f", "etc/galaxy-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	ctx := svc.NewServiceContext(c)

	// Run all job
	cr := cron.New()
	_ = cr.AddFunc("0 10 0 * * *", func() {
		logx.Info("========= Daily Sync Start ===========")
		defer Recover()
		_, e := clients.NewDmClient(ctx.EtcdClient).DeleteAll()
		if e != nil {
			logx.Error(e)
		}
	})
	cr.Start()
	defer cr.Stop()

	engine := rest.MustNewServer(c.RestConf)
	defer engine.Stop()

	handler.RegisterHandlers(engine, ctx)
	engine.Start()
}

func Recover() {
	err := recover()
	if err != nil {
		switch err.(type) {
		case runtime.Error:
			logx.Error("runtime error:", err)
		default:
			logx.Error("error:", err)
		}
	}
}
