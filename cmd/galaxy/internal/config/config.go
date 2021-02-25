package config

import "github.com/tal-tech/go-zero/rest"

type Config struct {
	rest.RestConf
	Mysql       string
	EtcdConfig  []string
	Debezium    string
	CanalConfig struct {
		UserName string
		Password string
		IP       string
		Port     string
		ServerID string
	}
	CkDataNodes []string
	DsnKey      string
	Auth        struct {
		AccessSecret string
	}
}
