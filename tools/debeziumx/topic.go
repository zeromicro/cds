package debeziumx

func GenerateTopic(db string, table string) string {
	return db + "." + table
}
