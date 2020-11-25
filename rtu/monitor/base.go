package monitor

import (
	"os"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// UNKNOWN 未知
	UNKNOWN = "unknown"
)

var (
	vecNameMap = make(map[string]struct{})
	vecNameMu  sync.Mutex
)

func hostname() string {
	host, err := os.Hostname()
	if err != nil {
		host = UNKNOWN
	}
	return host
}

func registerName(name ...string) {
	names := strings.Join(name, "-")
	vecNameMu.Lock()
	defer func() {
		vecNameMap[names] = struct{}{}
		vecNameMu.Unlock()
	}()
	if _, ok := vecNameMap[names]; ok {
		panic("prometheus duplicate name: " + names)
	}
}

// NewCounterVec 用于跟踪累计值，如某事件次数
func NewCounterVec(namespace, subsystem, name, help string, lables []string) (vec *prometheus.CounterVec) {
	registerName(namespace, subsystem, name)
	vec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        name,
			Help:        help,
			ConstLabels: prometheus.Labels{"host": hostname()},
		},
		lables,
	)
	prometheus.MustRegister(vec)
	return
}

// NewGaugeVec 用于数值变化，如内存变化
func NewGaugeVec(namespace, subsystem, name, help string, lables []string) (vec *prometheus.GaugeVec) {
	registerName(namespace, subsystem, name)
	vec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        name,
			Help:        help,
			ConstLabels: prometheus.Labels{"host": hostname()},
		},
		lables,
	)
	prometheus.MustRegister(vec)
	return
}

// NewSummaryVec 用于柱状图，用于跟踪请求耗时，响应大小，服务器端统计区间
func NewSummaryVec(namespace, subsystem, name, help string, lables []string) (vec *prometheus.SummaryVec) {
	registerName(namespace, subsystem, name)
	vec = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        name,
			Help:        help,
			ConstLabels: prometheus.Labels{"host": hostname()},
		},
		lables,
	)
	prometheus.MustRegister(vec)
	return
}

// NewHistogramVec 用于柱状图，用于跟踪请求耗时，响应大小，服务器端统计区间
func NewHistogramVec(namespace, subsystem, name, help string, lables []string, buckets []float64) (vec *prometheus.HistogramVec) {
	registerName(namespace, subsystem, name)
	vec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        name,
			Help:        help,
			ConstLabels: prometheus.Labels{"host": hostname()},
			Buckets:     buckets,
		},
		lables,
	)
	prometheus.MustRegister(vec)
	return
}
