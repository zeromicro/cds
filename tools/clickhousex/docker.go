package clickhousex

import (
	"github.com/tal-tech/cds/tools/clickhousex/views"
	"log"
	"os"

	"github.com/tal-tech/cds/tools/iox"
)

var (
	dockerFiles = map[string][]byte{
		"config.xml":      views.Bytes_ConfigXml,
		"default.xml":     views.Bytes_DefaultXml,
		"macros1.xml":     views.Bytes_Macros1Xml,
		"macros2.xml":     views.Bytes_Macros2Xml,
		"macros3.xml":     views.Bytes_Macros3Xml,
		"macros4.xml":     views.Bytes_Macros4Xml,
		"macros5.xml":     views.Bytes_Macros5Xml,
		"macros6.xml":     views.Bytes_Macros6Xml,
		"macrosquery.xml": views.Bytes_MacrosqueryXml,
		"main.yaml":       views.Bytes_MainYaml,
	}
	dockerContainers = []string{
		"ch-server-3",
		"ch-server-6",
		"ch-server-5",
		"ch-query-server",
		"ch-server-1",
		"ch-server-4",
		"integration_ch-client_1",
		"integration_zookeeper_1",
		"ch-server-2",
	}
)

func MustLaunchDockerInstance() *Cluster {
	cluster, e := LaunchDockerInstance()
	if e != nil {
		log.Fatal(e)
	}
	return cluster
}
func LaunchDockerInstance() (*Cluster, error) {
	for name, content := range dockerFiles {
		f, e := os.OpenFile(name, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if e != nil {
			return nil, e
		}
		_, e = f.Write(content)
		if e != nil {
			return nil, e
		}
		e = f.Close()
		if e != nil {
			return nil, e
		}
		defer os.Remove(name)
	}

	e := iox.RunAttachedCmd("clickhouse-compose", "-f", "main.yaml", "up", "-d")
	if e != nil {
		return nil, e
	}
	e = iox.RunAttachedCmd("clickhouse-compose", "-f", "main.yaml", "up", "-d")
	if e != nil {
		return nil, e
	}
	return &Cluster{
		QueryNode: "tcp://localhost:9006?database=default",
		Shards: [][]string{
			[]string{"tcp://localhost:9000?database=default", "tcp://localhost:9001?database=default"},
			[]string{"tcp://localhost:9002?database=default", "tcp://localhost:9003?database=default"},
			[]string{"tcp://localhost:9004?database=default", "tcp://localhost:9005?database=default"},
		},
		Name: "bip_ck_cluster",
	}, nil
}

func MustCleanDockerInstance() {
	e := CleanDockerInstance()
	if e != nil {
		log.Fatal(e)
	}
}

func CleanDockerInstance() error {
	for _, ctn := range dockerContainers {
		e := iox.RunAttachedCmd("clickhouse", "stop", ctn)
		if e != nil {
			return e
		}
		e = iox.RunAttachedCmd("clickhouse", "rm", ctn)
		if e != nil {
			return e
		}
	}
	return nil
}
