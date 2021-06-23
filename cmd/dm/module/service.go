package module

import (
	"context"
	"database/sql"
	"errors"
	"regexp"

	"github.com/tal-tech/go-zero/core/logx"
	"go.etcd.io/etcd/client/v3/concurrency"
	"gopkg.in/mgo.v2/bson"

	"github.com/tal-tech/cds/cmd/dm/choperator"
	"github.com/tal-tech/cds/cmd/dm/cmd/sync/config"
	"github.com/tal-tech/cds/cmd/dm/data"
	"github.com/tal-tech/cds/pkg/mongodbx"
)

type Service struct {
	Job        *config.Job
	Conf       *config.Config
	Controller *chan bool
	IDType     string
	Mutex      *concurrency.Mutex
	StopChan   *chan bool
}

func NewService(job *config.Job, config *config.Config, controller *chan bool, idType string, mutex *concurrency.Mutex, stopChan *chan bool) *Service {
	return &Service{Job: job, Conf: config, Controller: controller, IDType: idType, Mutex: mutex, StopChan: stopChan}
}

func (s *Service) Run() (string, error) {
	chOperator, err := choperator.NewChOperator(s.Job.Target.Shards)
	if err != nil {
		logx.Error(err)
		return "", err
	}
	switch s.Job.Source.Type {
	case "mongodb":
		var firstID string
		mongo, err := mongodbx.TakeMongoClient(s.Job.Source.Dsn)
		if err != nil {
			return "", err
		}
		logx.Info(`^` + s.Job.Source.Table + s.Job.Source.Suffix + `$`)
		// regular expression
		if s.Job.Source.Suffix != "" {
			db, err := mongodbx.ParseDsn(s.Job.Source.Dsn)
			if err != nil {
				logx.Error(err)
				return "", err
			}
			collections, err := mongo.Database(db.Database).ListCollectionNames(context.TODO(), bson.M{})
			if err != nil {
				logx.Error(err)
				return "", nil
			}
			for _, v := range collections {
				if ok, err := regexp.Match(`^`+s.Job.Source.Table+s.Job.Source.Suffix+`$`, []byte(v)); err == nil && ok {
					mongoModel := data.NewMongo2ClickHouseModel(mongo, &chOperator, s.Controller, s.Conf.Etcd.Hosts)
					firstID, err = mongoModel.MongoInertIntoClickHouse(s.Job, v, s.IDType, s.Conf.MongoBatchSize)
					if err != nil {
						logx.Error(err)
						return "", err
					}
				} else if err != nil {
					logx.Error(err)
					return "", nil
				}
			}
		} else {
			mongoModel := data.NewMongo2ClickHouseModel(mongo, &chOperator, s.Controller, s.Conf.Etcd.Hosts)
			firstID, err = mongoModel.MongoInertIntoClickHouse(s.Job, s.Job.Source.Table, s.IDType, s.Conf.MongoBatchSize)
			if err != nil {
				return "", err
			}
		}
		return firstID, nil
	case "mysql":
		mysql, err := sql.Open("mysql", s.Job.Source.Dsn)
		if err != nil {
			return "", err
		}
		defer func() {
			if err := mysql.Close(); err != nil {
				logx.Error(err)
			}
		}()

		mysqlModel := data.NewMysql2ClickHouseModel(mysql, &chOperator, s.Controller, s.Conf.Etcd.Hosts)
		firstID, err := mysqlModel.MysqlInertIntoClickHouse(s.Job)
		if err != nil {
			logx.Error(err)
		}
		return firstID, err
	default:
		return "", errors.New("db type not support")
	}

}
