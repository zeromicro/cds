package dbtool

func (s *DBTestToolMongo) Insert() ([]*DataInstance, error) {
	session, err := s.db.TakeSession()
	if err != nil {
		return nil, err
	}
	defer s.db.PutSession(session)
	collection := s.db.GetCollection(session)

	dataSet := GenerateDataSet(baseInsertNum)
	for _, item := range dataSet {
		err = collection.Insert(item)
		if err != nil {
			return nil, err
		}
	}

	return dataSet, err
}
