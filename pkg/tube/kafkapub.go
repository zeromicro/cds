package tube

import (
	"context"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type asyncProducer struct {
	producer *kafka.Writer
}

// MustNewAsyncProducer constructor
func MustNewAsyncProducer(topic string, brokerAddr []string) AsyncProducer {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokerAddr,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	})
	return &asyncProducer{
		producer: w,
	}
}

func (a *asyncProducer) AsyncProduce(ctx context.Context, s string) error {
	key := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	m := kafka.Message{
		Key:   []byte(key),
		Value: []byte(s),
	}

	return a.producer.WriteMessages(ctx, m)
}

func (a *asyncProducer) Close() error {
	return a.producer.Close()
}
