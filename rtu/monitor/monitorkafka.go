package monitor

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// KafkaVec prometheus vec
type KafkaVec struct {
	vec *prometheus.CounterVec
}

// KafkaLabels prometheus Kafka 类型 label
type KafkaLabels struct {
	Partition     int32
	Topic, Status string
	sync.Once
	Label prometheus.Labels
}

func (l *KafkaLabels) toPrometheusLable() prometheus.Labels {
	l.Do(func() {
		l.Label = prometheus.Labels{
			"partition": strconv.Itoa(int(l.Partition)),
			"topic":     l.Topic,
			"status":    l.Status,
		}
	})
	return l.Label
}

// NewKafkaVec prometheus kafka vec
func NewKafkaVec(namespace, subsystem, name string) *KafkaVec {
	dbVecCache.RLock()
	key := namespace + subsystem + name + "kfk"
	if vec, ok := dbVecCache.m[key]; ok {
		dbVecCache.RUnlock()
		return vec.(*KafkaVec)
	}
	dbVecCache.RUnlock()
	dbVecCache.Lock()
	if vec, ok := dbVecCache.m[key]; ok {
		dbVecCache.Unlock()
		return vec.(*KafkaVec)
	}
	dbVecCache.m[key] = &KafkaVec{
		vec: NewCounterVec(namespace, subsystem, name, "kafka counter by handlers", []string{"partition", "topic", "status"}),
	}
	dbVecCache.Unlock()
	return dbVecCache.m[key].(*KafkaVec)
}

// Inc inc label
func (kv *KafkaVec) Inc(labels *KafkaLabels) {
	kv.vec.With(labels.toPrometheusLable()).Inc()
}
