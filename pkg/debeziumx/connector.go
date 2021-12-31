package debeziumx

import (
	"encoding/json"
)

type (
	IConnector interface {
		Bytes() ([]byte, error)
	}
	MySQLConnector struct {
		Name   string `json:"name,omitempty"`
		Config struct {
			Class                string `json:"connector.class,omitempty"`
			TasksMax             uint   `json:"tasks.max,omitempty"`
			Hostname             string `json:"database.hostname,omitempty"`
			Port                 string `json:"database.port,omitempty"`
			User                 string `json:"database.user,omitempty"`
			Password             string `json:"database.password,omitempty"`
			ServerID             string `json:"database.server.id,omitempty"`
			ServerName           string `json:"database.server.name,omitempty"`
			WhiteList            string `json:"table.whitelist,omitempty"`
			KafkaSerevrs         string `json:"database.history.kafka.bootstrap.servers,omitempty"` // kafka:9092
			KafkaTopic           string `json:"database.history.kafka.topic,omitempty"`
			IncludeSchemaChanges bool   `json:"include.schema.changes,omitempty"`
		} `json:"config,omitempty"`
	}
	MongoConnector struct {
		Name   string `json:"name,omitempty"`
		Config struct {
			Class                       string `json:"connector.class,omitempty"`
			TasksMax                    uint   `json:"tasks.max,omitempty"`
			Topics                      string `json:"topics"`
			ConnectionUri               string `json:"connection.uri"`
			Collection                  string `json:"collection"`
			Database                    string `json:"database"`
			KeyConverter                string `json:"key.converter"`
			SchemasEnable               bool   `json:"key.converter.schemas.enable"`
			ValueConverter              string `json:"value.converter"`
			ValueConverterSchemasEnable bool   `json:"value.converter.schemas.enable"`
			MaxBatchSize                int    `json:"max.batch.size"`
			CopyExisting                bool   `json:"copy.existing"`
			ChangeDataCaptureHandler    string `json:"change.data.capture.handler"`
			Prefix                      string `json:"topic.prefix"`
			FullDocument                string `json:"publish.full.document.only"` // 当update时返回全字段而不是只有更新的字段
			Pipeline                    string `json:"pipeline"`
		} `json:"config,omitempty"`
	}
	Status struct {
		Name      string `json:"name,omitempty"`
		Connector struct {
			State    string `json:"state,omitempty"`
			WorkerId string `json:"worker_id,omitempty"`
		} `json:"connector,omitempty"`
		Tasks []struct {
			Id       int    `json:"id,omitempty"`
			State    string `json:"state,omitempty"`
			WorkerId string `json:"worker_id,omitempty"`
		} `json:"tasks,omitempty"`
		Type string `json:"type,omitempty"`
	}
)

func (m *MySQLConnector) Bytes() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MongoConnector) Bytes() ([]byte, error) {
	return json.Marshal(m)
}
