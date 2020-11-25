package monitor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// UnmarshalVec prometheus vec
type UnmarshalVec struct {
	vec *prometheus.CounterVec
}

// UnmarshalLabels prometheus Unmarshal 类型 label
type UnmarshalLabels struct {
	Category string
	Status   string // ok/failed/block/NonBlock
	sync.Once
	Label prometheus.Labels
}

func (l *UnmarshalLabels) toPrometheusLable() prometheus.Labels {
	l.Do(func() {
		l.Label = prometheus.Labels{
			"category": l.Category,
			"status":   l.Status,
		}
	})
	return l.Label
}

// NewUnmarshalVec prometheus Unmarshal vec
func NewUnmarshalVec(namespace, subsystem, name string) *UnmarshalVec {
	dbVecCache.RLock()
	key := namespace + subsystem + name + "db"
	if vec, ok := dbVecCache.m[key]; ok {
		dbVecCache.RUnlock()
		return vec.(*UnmarshalVec)
	}
	dbVecCache.RUnlock()
	dbVecCache.Lock()
	if vec, ok := dbVecCache.m[key]; ok {
		dbVecCache.Unlock()
		return vec.(*UnmarshalVec)
	}
	dbVecCache.m[key] = &UnmarshalVec{
		vec: NewCounterVec(namespace, subsystem, name, "Unmarshal counter by handlers", []string{"category", "status"}),
	}
	dbVecCache.Unlock()
	return dbVecCache.m[key].(*UnmarshalVec)
}

// Inc inc label
func (kv *UnmarshalVec) Inc(labels *UnmarshalLabels) {
	kv.vec.With(labels.toPrometheusLable()).Inc()
}
