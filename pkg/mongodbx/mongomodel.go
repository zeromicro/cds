package mongodbx

import (
	"context"
	"errors"
	"log"
	"reflect"

	"github.com/tal-tech/go-zero/core/logx"
	"github.com/zeromicro/cds/pkg/strx"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	MongoModel struct {
		Client     *mongo.Client
		AppName    string
		Dsn        string
		Database   string
		Collection string
		Type       reflect.Type
		Indexes    map[string]int

		FieldBsons []string
	}
)

func MustNewMongoModel(appName, dsn string, data interface{}) (*MongoModel, error) {
	b := &MongoModel{
		AppName: appName,
		Dsn:     dsn,
		Indexes: make(map[string]int),
	}
	info, e := ParseDsn(dsn)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	b.Database = info.Database
	b.Client, e = mongo.Connect(context.TODO(), options.Client().ApplyURI(dsn))
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = b.initData(data)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = b.Drop()
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = CreateIndex(b.Client.Database(b.Database).Collection(b.Collection), b.Indexes)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return b, nil
}

func NewMongoModel(appName, dsn string, data interface{}) (*MongoModel, error) {
	b := &MongoModel{
		AppName: appName,
		Dsn:     dsn,
	}
	info, e := ParseDsn(dsn)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	b.Database = info.Database
	b.Client, e = mongo.Connect(context.TODO(), options.Client().ApplyURI(dsn))
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = b.initData(data)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = CreateIndex(b.Client.Database(b.Database).Collection(b.Collection), b.Indexes)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return b, nil
}

func (m *MongoModel) initData(data interface{}) error {
	m.Type = reflect.TypeOf(data)
	if m.Type.Kind() == reflect.Ptr {
		m.Type = m.Type.Elem()
	}
	if m.AppName != "" {
		m.Collection = strx.ToSnakeCase(m.AppName) + "_" + strx.ToSnakeCase(m.Type.Name())
	} else {
		m.Collection = strx.ToSnakeCase(m.Type.Name())
	}

	for i := 0; i < m.Type.NumField(); i++ {
		field := m.Type.Field(i)
		// tag check
		bson, ok := field.Tag.Lookup("bson")
		if !ok {
			return errors.New(m.Type.Name() + "类型的" + field.Name + "字段没有写bson tag")
		}
		// if bson != "_id" && bson != strx.ToLowerCamel(field.Name) {
		// 	return errors.New(m.Type.Name() + "类型的" + field.Name + "字段bson tag不符合规范，应该是" + strx.ToLowerCamel(field.Name))
		// }

		m.FieldBsons = append(m.FieldBsons, bson)
		if idx, ok := field.Tag.Lookup("index"); ok {
			if idx == "-1" {
				m.Indexes[bson] = -1
			} else {
				m.Indexes[bson] = 1
			}
		}
	}
	return nil
}

// Drop drop collection
func (m *MongoModel) Drop() error {
	return m.Client.Database(m.Database).Collection(m.Collection).Drop(context.TODO())
}

func (m *MongoModel) Insert(v interface{}) error {
	_, e := m.Client.Database(m.Database).Collection(m.Collection).InsertOne(context.TODO(), v)
	return e
}

func (m *MongoModel) BatchInsert(vs interface{}) error {
	docs := []interface{}{}
	t := reflect.TypeOf(vs)
	if t.Kind() != reflect.Slice {
		return errors.New("BatchInsert必须传入一个切片类型的数据")
	}
	t = t.Elem()
	ptrMode := false
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		ptrMode = true
	}
	if t.Name() != m.Type.Name() {
		return errors.New("BatchInsert只能传入[]*" + m.Type.Name() + "类型的数据")
	}

	values := reflect.ValueOf(vs)
	for i := 0; i < values.Len(); i++ {
		v := values.Index(i)
		if ptrMode {
			v = v.Elem()
		}
		docs = append(docs, v.Interface())
	}

	_, e := m.Client.Database(m.Database).Collection(m.Collection).InsertMany(context.TODO(), docs)
	return e
}

func (m *MongoModel) UpdateSets(where, sets bson.M) (int64, error) {
	result, e := m.Client.Database(m.Database).Collection(m.Collection).UpdateMany(context.TODO(), where, bson.M{
		"$set": sets,
	})
	if e != nil {
		return 0, e
	}
	return result.ModifiedCount, nil
}

func (m *MongoModel) DeleteWhere(where bson.M) (int64, error) {
	result, e := m.Client.Database(m.Database).Collection(m.Collection).DeleteMany(context.TODO(), where)
	if e != nil {
		return 0, e
	}
	return result.DeletedCount, nil
}

func (m *MongoModel) QueryWhere(where bson.M) (interface{}, error) {
	if where == nil {
		where = bson.M{}
	}
	t := reflect.SliceOf(reflect.PtrTo(m.Type))
	vs := reflect.New(t).Interface()
	cursor, e := m.Client.Database(m.Database).Collection(m.Collection).Find(context.TODO(), where)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	e = cursor.All(context.TODO(), vs)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return reflect.ValueOf(vs).Elem().Interface(), nil
}

func (m *MongoModel) All() (interface{}, error) {
	return m.QueryWhere(nil)
}

func (m *MongoModel) MustAll() interface{} {
	vs, e := m.All()
	if e != nil {
		log.Fatal(e)
	}
	return vs
}
