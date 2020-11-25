package mysqlx

import (
	"log"
	"os"

	"cds/tools/iox"
	"cds/tools/mysqlx/views"
)

func MustLaunchDockerInstance() string {
	dsn, e := LaunchDockerInstance()
	if e != nil {
		log.Fatal(e)
	}
	return dsn
}

func LaunchDockerInstance() (string, error) {
	filename := ".mysql.yaml"
	file, e := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if e != nil {
		return "", e
	}
	_, e = file.Write(views.Bytes_MysqlYml)
	if e != nil {
		return "", e
	}
	e = file.Close()
	if e != nil {
		return "", e
	}

	e = iox.RunAttachedCmd("clickhouse-compose", "-f", filename, "up", "-d")
	if e != nil {
		return "", e
	}
	e = os.Remove(filename)
	if e != nil {
		return "", e
	}
	return "root:1234@tcp(127.0.0.1:3307)/mysqlx", nil
}

func MustCleanDockerInstance() {
	e := CleanDockerInstance()
	if e != nil {
		log.Fatal(e)
	}
}

func CleanDockerInstance() error {
	e := iox.RunAttachedCmd("clickhouse", "stop", "mysql")
	if e != nil {
		return e
	}
	return iox.RunAttachedCmd("clickhouse", "rm", "mysql")
}
