package tube

import "context"

// Producer kafka client to produce message
type Producer interface {
	Produce(string, string) error
	Close() error
}

// AsyncProducer kafka client to async produce message
type AsyncProducer interface {
	AsyncProduce(context.Context, string) error
	Close() error
}

// HandleFunc receive kafka message ,return result
type HandleFunc func([]byte) (interface{}, error)

// KfkStreamConsumer  aim to accelerate process speed per consumer
type KfkStreamConsumer interface {
	// start to async fetch the messages .
	// when the amount of received message is up to the config num,
	// the caller can get the slice of subscribe messages
	Subscribe(ctx context.Context, handle HandleFunc) chan interface{}

	// Commit the continuously outputted messages' offset
	Commit() error

	// stop  fetching  message
	Close() error
}
