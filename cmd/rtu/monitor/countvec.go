package monitor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	cntVecCache = &vecCache{
		m: make(map[string]interface{}),
	}
}

var cntVecCache *vecCache

// CountVec prometheus vec
type CountVec struct {
	vec *prometheus.CounterVec
}

// CountLabels prometheus Database 类型 label
type CountLabels struct {
	Status string // ok/failed/block/NonBlock
	Action string
	sync.Once
	label prometheus.Labels
}

func (l *CountLabels) toPrometheusLable() prometheus.Labels {
	l.Do(func() {
		l.label = prometheus.Labels{
			"status": l.Status,
			"action": l.Action,
		}
	})
	return l.label
}

// NewCountVec prometheus Database vec
func NewCountVec(namespace, subsystem, name string) *CountVec {
	cntVecCache.RLock()
	key := namespace + subsystem + name + "cnt"
	if vec, ok := cntVecCache.m[key]; ok {
		cntVecCache.RUnlock()
		return vec.(*CountVec)
	}
	cntVecCache.RUnlock()
	cntVecCache.Lock()
	if vec, ok := cntVecCache.m[key]; ok {
		cntVecCache.Unlock()
		return vec.(*CountVec)
	}
	cntVecCache.m[key] = &CountVec{
		vec: NewCounterVec(namespace, subsystem, name, "Database counter by handlers", []string{"status", "action"}),
	}
	cntVecCache.Unlock()
	return cntVecCache.m[key].(*CountVec)
}

// Inc inc label
func (kv *CountVec) Inc(labels *CountLabels) {
	kv.vec.With(labels.toPrometheusLable()).Inc()
}
