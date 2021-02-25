package dbtool

import (
	"fmt"

	"github.com/globalsign/mgo/bson"
)

func (s *DBTestToolMongo) Clean() error {
	session, err := s.db.TakeSession()
	if err != nil {
		return err
	}
	defer s.db.PutSession(session)
	_, err = session.DB(s.Database).C(s.Collection).RemoveAll(bson.M{})
	return err
}

func (s *DBTestToolMongo) SetUp() error { return nil }

func (s *DBTestToolMongo) Delete() ([]*DataInstance, error) {
	fmt.Println("mongo delete")
	session, err := s.db.TakeSession()
	if err != nil {
		return nil, err
	}
	defer s.db.PutSession(session)
	collection := s.db.GetCollection(session)
	dataSet, err := s.Insert()
	if err != nil {
		return nil, err
	}
	ids := []int64{}
	for _, v := range dataSet {
		ids = append(ids, v.PK)
	}
	filter := bson.M{"pk": bson.M{"$in": ids}}
	if _, err := collection.RemoveAll(filter); err != nil {
		return nil, err
	}
	return dataSet, nil
}

func (s *DBTestToolMongo) Update() ([]*DataInstance, error) {
	session, err := s.db.TakeSession()
	if err != nil {
		return nil, err
	}
	defer s.db.PutSession(session)
	collection := s.db.GetCollection(session)
	dataSet, err := s.Insert()
	if err != nil {
		return nil, err
	}
	dataSet = UpdateDataSet(dataSet)
	for _, item := range dataSet {
		filter := bson.M{"pk": item.PK}
		updater := bson.M{"$set": bson.M{"int_value": item.IntValue,
			"float_value":   item.FloatValue,
			"double_value":  item.DoubleValue,
			"char_value":    item.CharValue,
			"varchar_value": item.VarCharValue,
			"time_value":    item.TimeValue}}
		if err := collection.Update(filter, updater); err != nil {
			return nil, err
		}
	}
	return dataSet, nil
}

func (s *DBTestToolMongo) Select(dataSet []*DataInstance) (map[int64]int, error) {
	return nil, nil
}
