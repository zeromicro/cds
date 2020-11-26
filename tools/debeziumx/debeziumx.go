package debeziumx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/tal-tech/go-zero/core/logx"

	"github.com/tal-tech/cds/tools/numx"
	"github.com/tal-tech/cds/tools/strx"
)

type Debeziumx struct {
	Addr   string
	Host   string
	client http.Client
}

// NewDebeziumx NewDebeziumx("http://127.0.0.1:8083")
func NewDebeziumx(addr string) (*Debeziumx, error) {
	if !strings.HasPrefix(addr, "http://") {
		addr = "http://" + addr
	}
	d := &Debeziumx{
		Addr: addr,
	}
	u, e := url.Parse(addr)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	d.Host = strx.SubBeforeLast(u.Host, ":", u.Host)
	return d, nil
}

func (d *Debeziumx) Version() (string, error) {
	r, e := http.NewRequest(http.MethodGet, d.Addr, nil)
	if e != nil {
		return "", e
	}
	rp, e := d.client.Do(r)
	if e != nil {
		return "", e
	}
	defer rp.Body.Close()
	b, e := ioutil.ReadAll(rp.Body)
	if e != nil {
		return "", e
	}
	v := struct {
		Version string `json:"Version"`
	}{}
	e = json.Unmarshal(b, &v)
	if e != nil {
		return "", e
	}
	return v.Version, nil
}

func (d *Debeziumx) Connectors() ([]string, error) {
	r, e := http.NewRequest(http.MethodGet, d.Addr+"/connectors/", nil)
	if e != nil {
		return nil, e
	}
	rp, e := d.client.Do(r)
	if e != nil {
		return nil, e
	}
	defer rp.Body.Close()
	b, e := ioutil.ReadAll(rp.Body)
	if e != nil {
		return nil, e
	}
	vs := []string{}
	e = json.Unmarshal(b, &vs)
	if e != nil {
		return nil, e
	}
	return vs, nil
}

func (d *Debeziumx) Listen(cfg IConnector) error {
	b, e := cfg.Bytes()
	if e != nil {
		logx.Error(e)
		return e
	}
	r, e := http.NewRequest(http.MethodPost, d.Addr+"/connectors/", bytes.NewReader(b))
	if e != nil {
		logx.Error(e)
		return e
	}
	r.Header.Set("Content-Type", "application/json")
	rp, e := d.client.Do(r)
	if e != nil {
		logx.Error(e)
		return e
	}

	defer rp.Body.Close()
	b, e = ioutil.ReadAll(rp.Body)
	if e != nil {
		logx.Error(e)
		return e
	}
	str := string(b)
	if rp.StatusCode >= 300 {
		return errors.New("code=" + strconv.Itoa(rp.StatusCode) + "," + str)
	}
	return nil
}

func (d *Debeziumx) DeleteConnector(name string) error {
	r, e := http.NewRequest(http.MethodDelete, d.Addr+"/connectors/"+name, nil)
	if e != nil {
		logx.Error(e)
		return e
	}
	rp, e := d.client.Do(r)
	if e != nil {
		logx.Error(e)
		return e
	}
	defer rp.Body.Close()
	b, e := ioutil.ReadAll(rp.Body)
	if e != nil {
		logx.Error(e)
		return e
	}
	str := string(b)
	if rp.StatusCode >= 300 {
		return errors.New(strconv.Itoa(rp.StatusCode) + rp.Status + str)
	}
	return nil
}

func (d *Debeziumx) CheckConnectorExists(name string) (bool, error) {
	r, e := http.NewRequest(http.MethodDelete, d.Addr+"/connectors/"+name, nil)
	if e != nil {
		logx.Error(e)
		return false, e
	}
	rp, e := d.client.Do(r)
	if e != nil {
		logx.Error(e)
		return false, e

	}
	defer rp.Body.Close()
	b, e := ioutil.ReadAll(rp.Body)
	if e != nil {
		logx.Error(e)
		return false, e

	}
	switch rp.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		logx.Errorf("check connector code is:[%d], msg:[%s]", rp.StatusCode, string(b))
		return true, nil
	}
}

func (d *Debeziumx) Status(name string) (*Status, error) {
	r, e := http.NewRequest(http.MethodGet, d.Addr+"/connectors/"+name+"/status", nil)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	rp, e := d.client.Do(r)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	defer rp.Body.Close()
	b, e := ioutil.ReadAll(rp.Body)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	str := string(b)
	if rp.StatusCode != 200 {
		return nil, errors.New(str)
	}
	sts := Status{}
	e = json.Unmarshal(b, &sts)
	if e != nil {
		logx.Error(e)
		return nil, e
	}
	return &sts, nil
}

func (d *Debeziumx) ListenMySQL(dsn, table string) (string, error) {
	info, e := mysql.ParseDSN(dsn)
	if e != nil {
		logx.Error(e)
		return "", e
	}
	cfg := &MySQLConnector{
		Name: info.DBName + "." + table,
	}
	cfg.Config.Class = "io.debezium.connector.mysql.MySqlConnector"
	cfg.Config.TasksMax = 1
	cfg.Config.Hostname = strx.SubBeforeLast(info.Addr, ":", info.Addr)
	cfg.Config.Port = strx.SubAfterLast(info.Addr, ":", "3306")
	cfg.Config.User = info.User
	cfg.Config.Password = info.Passwd
	cfg.Config.ServerID = strconv.Itoa(numx.Randn(60000))
	cfg.Config.ServerName = "debeziumx"
	cfg.Config.WhiteList = info.DBName + "." + table
	cfg.Config.KafkaSerevrs = d.Host + ":9092"
	cfg.Config.KafkaTopic = GenerateTopic(info.DBName, table)
	e = d.Listen(cfg)
	if e != nil {
		logx.Error(e)
		return "", e
	}
	return cfg.Config.ServerName + "." + cfg.Config.KafkaTopic, nil
}

func (d *Debeziumx) ListenMongo(dsn, db, collection, suffix string) (string, error) {
	cfg := d.buildConf(dsn, db, collection, suffix)
	e := d.Listen(cfg)
	if e != nil {
		logx.Error(e)
		return "", e
	}
	return cfg.Name, nil
}

func (d *Debeziumx) buildConf(dsn, db, collection, suffix string) *MongoConnector {
	cfg := &MongoConnector{
		Name: "mongoconnector." + db + "." + collection,
	}
	cfg.Config.Class = "com.mongodb.kafka.connect.MongoSourceConnector"
	cfg.Config.TasksMax = 1
	topic := cfg.Name
	cfg.Config.Topics = topic
	cfg.Config.Prefix = "mongoconnector"
	cfg.Config.ConnectionUri = dsn
	cfg.Config.Collection = collection
	cfg.Config.Database = db
	cfg.Config.KeyConverter = "org.apache.kafka.connect.json.JsonConverter"
	cfg.Config.ValueConverter = "org.apache.kafka.connect.json.JsonConverter"
	cfg.Config.MaxBatchSize = 1
	cfg.Config.CopyExisting = false
	cfg.Config.FullDocument = "true"
	cfg.Config.ChangeDataCaptureHandler = "com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler"
	if len(suffix) != 0 {
		cfg.Config.Pipeline = fmt.Sprintf(`[{"$match": {"ns.coll": {"$regex": "(^%s$|^%s%s$)"}}}]`, collection, collection, suffix)
		cfg.Config.Collection = ""
	}
	return cfg
}
