package monitor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var vecCached *vecCache

func init() {
	vecCached = &vecCache{
		m: make(map[string]interface{}),
	}
}

// GaugeVec prometheus vec
type GaugeVec struct {
	vec *prometheus.GaugeVec
}

// GaugeLabels prometheus Database 类型 label
type GaugeLabels struct {
	Db       string
	Table    string
	Category string
	sync.Once
	label prometheus.Labels
}

func (l *GaugeLabels) toPrometheusLable() prometheus.Labels {
	l.Do(func() {
		l.label = prometheus.Labels{
			"db":       l.Db,
			"table":    l.Table,
			"category": l.Category,
		}
	})
	return l.label
}

// NewGaugerVec prometheus Database vec
func NewGaugerVec(namespace, subsystem, name string, labels ...string) *GaugeVec {
	vecCached.RLock()
	key := namespace + subsystem + name + "cnt"
	if vec, ok := vecCached.m[key]; ok {
		vecCached.RUnlock()
		return vec.(*GaugeVec)
	}
	vecCached.RUnlock()
	vecCached.Lock()
	if vec, ok := vecCached.m[key]; ok {
		vecCached.Unlock()
		return vec.(*GaugeVec)
	}
	vecCached.m[key] = &GaugeVec{
		vec: NewGaugeVec(namespace, subsystem, name, "", labels),
	}
	vecCached.Unlock()
	return vecCached.m[key].(*GaugeVec)
}

// Set inc label
func (kv *GaugeVec) Set(labels *GaugeLabels, value int) {
	v := float64(value)
	kv.vec.With(labels.toPrometheusLable()).Set(v)
}
