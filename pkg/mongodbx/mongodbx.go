package mongodbx

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

func ParseDsn(dsn string) (connstring.ConnString, error) {
	info, e := connstring.Parse(dsn)
	if e != nil {
		return connstring.ConnString{}, e
	}
	return info, nil
}

// CreateIndex creates indexes for coll
func CreateIndex(coll *mongo.Collection, indexes map[string]int) error {
	if len(indexes) == 0 {
		return nil
	}
	imodels := []mongo.IndexModel{}
	for k, v := range indexes {
		sequence := bsonx.Int32(1)
		if v < 0 {
			sequence = bsonx.Int32(-1)
		}
		imodel := mongo.IndexModel{
			Keys: bsonx.Doc{bsonx.Elem{Key: k, Value: sequence}},
		}
		imodels = append(imodels, imodel)
	}

	_, e := coll.Indexes().CreateMany(context.TODO(), imodels)
	return e
}

func ListCollections(client *mongo.Client, dsn string) ([]string, error) {
	info, e := connstring.Parse(dsn)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	names, e := client.Database(info.Database).ListCollectionNames(context.TODO(), bson.M{})
	if e != nil {
		return nil, e
	}
	return names, nil
}

func CountDocuments(client *mongo.Client, dsn, coll string) (int64, error) {
	info, e := connstring.Parse(dsn)
	if e != nil {
		return 0, e
	}
	count, e := client.Database(info.Database).Collection(coll).CountDocuments(context.TODO(), bson.M{})
	if e != nil {
		return 0, e
	}
	return count, nil
}

// CollectionExists check if collection exists
func CollectionExists(db *mongo.Database, coll string) (bool, error) {
	names, e := db.ListCollectionNames(context.TODO(), bson.M{})
	if e != nil {
		return false, e
	}

	for _, name := range names {
		if name == coll {
			return true, nil
		}
	}
	return false, nil
}

func CollectionsMatchTimePattern(db *mongo.Database, pattern string) ([]string, error) {
	names, e := db.ListCollectionNames(context.TODO(), bson.M{})
	if e != nil {
		return nil, e
	}

	vs := []string{}
	for _, name := range names {
		_, e := time.Parse(pattern, name)
		if e != nil {
			continue
		}
		vs = append(vs, name)
	}
	return vs, nil
}

// CreateIndexIfNotExists create indexes if collection doesn't exists
func CreateIndexIfNotExists(db *mongo.Database, collname string, indexes map[string]int) error {
	b, e := CollectionExists(db, collname)
	if e != nil {
		return e
	}
	if b {
		return nil
	}

	coll := db.Collection(collname)

	return CreateIndex(coll, indexes)
}
