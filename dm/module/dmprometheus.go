package module

import (
	prometheus2 "github.com/prometheus/client_golang/prometheus"
	"github.com/tal-tech/go-zero/core/logx"
	"github.com/tal-tech/go-zero/core/prometheus"
)

type DmPrometheus struct {
	proConf *prometheus.Config
	onOff   chan bool
}

var CountOfJobFinished = prometheus2.NewCounterVec(
	prometheus2.CounterOpts{
		Name: "dm_task_finished",
		Help: "Number of Job Finished By Dm",
	},
	[]string{},
)

var CountOfJobError = prometheus2.NewCounterVec(
	prometheus2.CounterOpts{
		Name: "dm_task_error",
		Help: "Number of Job Occurred Error In Dm",
	},
	[]string{},
)

var CountOfJob = prometheus2.NewCounterVec(
	prometheus2.CounterOpts{
		Name: "dm_task_total_count",
		Help: "Number of Job",
	},
	[]string{},
)

var CountOfRunningGoRoutine = prometheus2.NewGauge(
	prometheus2.GaugeOpts{
		Name: "dm_go_routine_count",
		Help: "Number of Go Routine Running",
	},
)

func NewDmPrometheus(pc *prometheus.Config, onOff chan bool) *DmPrometheus {
	return &DmPrometheus{proConf: pc, onOff: onOff}
}

func (dp *DmPrometheus) Run() {
	select {
	case <-dp.onOff:
		logx.Info("prometheus shut down")
		return
	default:
		prometheus2.MustRegister(CountOfJobFinished)
		prometheus2.MustRegister(CountOfJobError)
		prometheus2.MustRegister(CountOfRunningGoRoutine)
		prometheus2.MustRegister(CountOfJob)
		prometheus.StartAgent(*dp.proConf)
	}
}

func (dp *DmPrometheus) Stop() {
	dp.onOff <- true
}

func IncCountOfTaskFinished() {
	CountOfJobFinished.WithLabelValues().Inc()
}

func IncCountOfTaskError() {
	CountOfJobError.WithLabelValues().Inc()
}

func IncCountOfTask() {
	CountOfJob.WithLabelValues().Inc()
}

func IncCountOfRunningGoRoutine() {
	CountOfRunningGoRoutine.Inc()
}

func DecCountOfRunningGoRoutine() {
	CountOfRunningGoRoutine.Dec()
}
