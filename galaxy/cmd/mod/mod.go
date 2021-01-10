package mod

import (
	"flag"

	"github.com/tal-tech/cds/galaxy/internal/config"
	"github.com/tal-tech/cds/galaxy/internal/handler"
	"github.com/tal-tech/cds/galaxy/internal/svc"
	"github.com/tal-tech/go-zero/core/conf"
	"github.com/tal-tech/go-zero/rest"
)

var configFile = flag.String("f", "etc/galaxy-api.yaml", "the config file")

func Mod() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	ctx := svc.NewServiceContext(c)

	engine := rest.MustNewServer(c.RestConf)
	defer engine.Stop()

	handler.RegisterHandlers(engine, ctx)
	engine.Start()
}
