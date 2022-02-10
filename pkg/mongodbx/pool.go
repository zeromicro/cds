package mongodbx

import (
	"context"
	"sync"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoPool = sync.Map{}

func TakeMongoClient(dsn string) (*mongo.Client, error) {
	client, ok := mongoPool.Load(dsn)
	if !ok {
		client, e := mongo.Connect(context.TODO(), options.Client().ApplyURI(dsn))
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		mongoPool.Store(dsn, client)
		return client, nil
	}
	return client.(*mongo.Client), nil
}
