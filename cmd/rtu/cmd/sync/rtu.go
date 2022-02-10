package main

import (
	"flag"
	"log"
	_ "net/http/pprof" //  #nosec
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/zeromicro/cds/cmd/rtu/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/rtu/handle"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	_ "github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/prometheus"
)

func showVersion() {
	logx.Infof("version: %s\n", version)
	logx.Infof("commitId: %s\n", commitid)
	logx.Infof("build time: %s\n", buildtime)
	logx.Infof("%s\n", goversion)
}

var (
	version   = "undefined"
	commitid  = "undefined"
	buildtime = "undefined"
	goversion = "undefined"

	configPath = flag.String("f", "etc/rtu.json", "the config file")
	configFile config.Config
)

func main() {
	flag.Parse()
	if err := conf.LoadConfig(*configPath, &configFile); err != nil {
		logx.Info(err)
	}
	logx.MustSetup(configFile.Log)
	args := os.Args
	if len(args) > 1 {
		for _, arg := range args {
			if arg == "-v" || arg == "--version" {
				showVersion()
				return
			}
		}
	}
	go func() {
		tick := time.NewTicker(10 * time.Second)
		for range tick.C {
			printUsage()
		}
	}()
	handle.SetConfig(&configFile)
	exit := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go pprof(exit, &wg)
	// 主线程
	go handle.Run(exit, &wg)
	await(exit, &wg)

	logx.Info("server gracefully shutdown")
}

func pprof(exit chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	prometheus.StartAgent(configFile.Prometheus)
	<-exit

	logx.Info("shutdown pprof server gracefully")
}

func await(exit chan struct{}, wg *sync.WaitGroup) {
	// wait signal to shutdown gracefully
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	log.Printf("receive signal `%s`", <-sig)

	close(exit)

	// time.Sleep(conf.BATCH_TIME * 5)
	logx.Info("waiting...")
	wg.Wait()

	logx.Info("waiiting ok")
}

func bToMb(b uint64) float32 {
	return float32(b) / 1024 / 1024
}

func printUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logx.Infof("MEMORY: Alloc=%.1fMi, TotalAlloc=%.1fMi, Sys=%.1fMi, NumGC=%d",
		bToMb(m.Alloc), bToMb(m.TotalAlloc), bToMb(m.Sys), m.NumGC)
}
