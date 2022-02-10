package wrap

import (
	"flag"

	"github.com/zeromicro/cds/cmd/galaxy/internal/config"
	"github.com/zeromicro/cds/cmd/galaxy/internal/handler"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
	"github.com/zeromicro/cds/pkg/strx"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/galaxy-api.yaml", "the config file")

func Wrap() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	strx.SetDsnKey(c.DsnKey)
	ctx := svc.NewServiceContext(c)

	engine := rest.MustNewServer(c.RestConf)
	defer engine.Stop()

	handler.RegisterHandlers(engine, ctx)
	engine.Start()
}
