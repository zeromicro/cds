package dbtool

const deleteSQL = "delete from test.test_data where pk=?"

func (s *DBTestToolSqlConn) Delete() ([]*DataInstance, error) {
	dataSet, err := s.Insert()
	if err != nil {
		return nil, err
	}
	for _, item := range dataSet {
		_, err := s.db.Exec(deleteSQL, item.PK)
		if err != nil {
			return nil, err
		}
	}
	return dataSet, nil
}
