package debeziumx

func GenerateTopic(db, table string) string {
	return db + "." + table
}
