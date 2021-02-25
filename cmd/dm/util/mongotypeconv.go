package util

import (
	"encoding/json"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"gopkg.in/mgo.v2/bson"
)

func convertBsonType2GoType(kt []string, defaultTypeMap map[string]interface{}) ([]interface{}, string, int, error) {
	var pic []interface{}
	id, tp := -1, "string"
	for k, v := range kt {
		if v == "insert_id" {
			intNum, err := strconv.Atoi(strconv.FormatInt(time.Now().UnixNano(), 10))
			if err != nil {
				return nil, "", 0, err
			}
			pic = append(pic, uint64(intNum))
		} else {
			switch vv := defaultTypeMap[v].(type) {
			case bool:
				pic = append(pic, strconv.FormatBool(vv))
			case primitive.DateTime:
				pic = append(pic, vv.Time().In(ShangHaiLocation))
			case primitive.ObjectID:
				if v == "_id" {
					id, tp = k, "ObjectId"
				}
				pic = append(pic, vv.Hex())
			case primitive.A:
				bt, err := json.Marshal(vv)
				if err != nil {
					return nil, "", 0, err
				}
				pic = append(pic, string(bt))
			case bson.M:
				json, err := json.Marshal(vv)
				if err != nil {
					return nil, "", 0, err
				}
				pic = append(pic, string(json))
			default:
				if v == "_id" {
					id = k
				}
				pic = append(pic, defaultTypeMap[v])
			}
		}
	}
	return pic, tp, id, nil
}
