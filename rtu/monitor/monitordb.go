package monitor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type vecCache struct {
	m map[string]interface{}
	sync.RWMutex
}

var dbVecCache *vecCache

func init() {
	dbVecCache = &vecCache{
		m: make(map[string]interface{}),
	}
}

// DatabaseVec prometheus vec
type DatabaseVec struct {
	vec *prometheus.CounterVec
}

// DatabaseLabels prometheus Database 类型 label
type DatabaseLabels struct {
	Table  string
	Status string // ok/failed/block/NonBlock
	Action string
	sync.Once
	Label prometheus.Labels
}

func (l *DatabaseLabels) toPrometheusLable() prometheus.Labels {
	l.Do(func() {
		l.Label = prometheus.Labels{
			"table":  l.Table,
			"status": l.Status,
			"action": l.Action,
		}
	})
	return l.Label
}

// NewDatabaseVec prometheus Database vec
func NewDatabaseVec(namespace, subsystem, name string) *DatabaseVec {
	dbVecCache.RLock()
	key := namespace + subsystem + name + "db"
	if vec, ok := dbVecCache.m[key]; ok {
		dbVecCache.RUnlock()
		return vec.(*DatabaseVec)
	}
	dbVecCache.RUnlock()
	dbVecCache.Lock()
	if vec, ok := dbVecCache.m[key]; ok {
		dbVecCache.Unlock()
		return vec.(*DatabaseVec)
	}
	dbVecCache.m[key] = &DatabaseVec{
		vec: NewCounterVec(namespace, subsystem, name, "Database counter by handlers", []string{"table", "status", "action"}),
	}
	dbVecCache.Unlock()
	return dbVecCache.m[key].(*DatabaseVec)
}

// Inc inc label
func (kv *DatabaseVec) Inc(labels *DatabaseLabels) {
	kv.vec.With(labels.toPrometheusLable()).Inc()
}
