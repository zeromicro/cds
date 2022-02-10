package data

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/zeromicro/cds/cmd/dm/choperator"
	"github.com/zeromicro/cds/cmd/dm/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/dm/util"
	"github.com/zeromicro/cds/pkg/mongodbx"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

type (
	Mongo2ClickHouseModel struct {
		mongoConn  *mongo.Client
		chOperator *choperator.ChOperator
		controller *chan bool
		etcdHosts  []string
	}
	IDType struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
)

func NewMongo2ClickHouseModel(mongoConn *mongo.Client, chOperator *choperator.ChOperator, controller *chan bool, etcdHosts []string) *Mongo2ClickHouseModel {
	return &Mongo2ClickHouseModel{mongoConn: mongoConn, chOperator: chOperator, controller: controller, etcdHosts: etcdHosts}
}

func (mg *Mongo2ClickHouseModel) MongoInertIntoClickHouse(job *config.Job, tableName, idType string, batchSize int) (string, error) {
	var bs []bson.M
	if len(idType) != 0 {
		if obj, err := GetTypeID(idType); err == nil {
			bs = append(bs, bson.M{
				"_id": bson.M{"$gte": obj},
			})
		} else {
			logx.Error(err)
			return "", err
		}
	}
	db, err := mongodbx.ParseDsn(job.Source.Dsn)
	if err != nil {
		logx.Error(err)
		return "", err
	}
	cursor, err := mg.mongoConn.Database(db.Database).Collection(tableName).Aggregate(context.TODO(), bs,
		options.Aggregate().SetAllowDiskUse(true))
	defer func() {
		if err := cursor.Close(context.TODO()); err != nil {
			logx.Error(err)
		}
	}()
	if err != nil {
		return "", err
	}
	name2Type, err := (*mg.chOperator).ObtainClickHouseKV(job.Target.DB, job.Target.Table)
	if err != nil {
		logx.Error(err)
		return "", err
	}
	insertQuery := combineQuery(name2Type, job.Target.DB, job.Target.Table)
	insertData := make([][]interface{}, 0, 5e4)
	var indexOfID, totalSize int
	for cursor.Next(context.TODO()) {
		// 创建一个值，将单个文档解码为该值
		var elem bson.M
		err := cursor.Decode(&elem)
		if err != nil {
			logx.Error(err)
			return "", err
		}
		totalSize += len(cursor.Current)
		// this is the all k,v
		data, tp, indexOfID, err := util.RepairData(elem, name2Type)
		if err != nil {
			logx.Error(err)
			return "", err
		}
		if indexOfID == -1 {
			return "", errors.New("cannot obtain id of mongodb")
		}
		insertData = append(insertData, data)
		if len(insertData) == 5e4 || totalSize > batchSize {
			select {
			case <-*mg.controller:
				logx.Info("Task ID:" + job.ID + " has been stopped manually")
				return "stopped", nil
			default:
				// 如果到了窗口期 记录一下预备写入的第一条ID 并停止
				if job.WindowPeriod.EndHour == 0 && job.WindowPeriod.StartHour == 0 {
				} else if time.Now().Hour() > job.WindowPeriod.EndHour || time.Now().Hour() < job.WindowPeriod.StartHour {
					if firstID, ok := insertData[0][indexOfID].(string); ok {
						if id2Type, err := json.Marshal(IDType{ID: firstID, Type: tp}); err == nil {
							return string(id2Type), nil
						}
					}
					return "", errors.New("cannot convert [id] interface to string ")
				}

				if err := (*mg.chOperator).BatchInsert(insertData, insertQuery, indexOfID); err != nil {
					logx.Error(err)
					return "", err
				}
				insertData, totalSize = insertData[:0], 0
			}
		}
	}
	if len(insertData) > 0 {
		if err := (*mg.chOperator).BatchInsert(insertData, insertQuery, indexOfID); err != nil {
			return "", err
		}
	}
	return "", nil
}

func combineQuery(mp map[string]string, db, targetTable string) string {
	var insertSqlBuilder strings.Builder
	insertSqlBuilder.WriteString("insert into " + db + "." + targetTable + " (")
	var suffix string
	var des []string
	kt := make([]string, 0, len(mp))
	for k := range mp {
		kt = append(kt, k)
	}
	sort.Strings(kt)
	for i := 0; i < len(kt); i++ {
		des = append(des, "`"+kt[i]+"`")
	}
	for i := 0; i < len(des); i++ {
		suffix += "?"
		insertSqlBuilder.WriteString(des[i])
		if i != len(des)-1 {
			insertSqlBuilder.WriteString(",")
			suffix += ","
		}
	}
	insertSqlBuilder.WriteString(") values (" + suffix + ")")
	return insertSqlBuilder.String()
}

func GetTypeID(s string) (interface{}, error) {
	var idType IDType
	if err := json.Unmarshal([]byte(s), &idType); err != nil {
		return "", err
	}
	switch idType.Type {
	case "ObjectId":
		obj, err := primitive.ObjectIDFromHex(idType.ID)
		if err != nil {
			return "", err
		}
		return obj, nil
	case "String":
		return s, nil
	default:
		return nil, errors.New("start id type illegal")
	}
}
