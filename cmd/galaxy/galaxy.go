package main

import (
	"flag"
	"fmt"

	"github.com/tal-tech/go-zero/core/conf"
	"github.com/tal-tech/go-zero/rest"
	"github.com/zeromicro/cds/cmd/galaxy/internal/config"
	"github.com/zeromicro/cds/cmd/galaxy/internal/handler"
	"github.com/zeromicro/cds/cmd/galaxy/internal/svc"
)

var configFile = flag.String("f", "etc/galaxy-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	ctx := svc.NewServiceContext(c)
	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
