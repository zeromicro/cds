package logic

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tal-tech/go-zero/core/logx"
	config2 "github.com/zeromicro/cds/cmd/dm/cmd/sync/config"
	"github.com/zeromicro/cds/cmd/galaxy/internal/model"
	"github.com/zeromicro/cds/cmd/rtu/cmd/sync/config"
	"github.com/zeromicro/cds/pkg/mysqlx"
	"github.com/zeromicro/cds/pkg/strx"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

func buildJob(dsn, id, sourceType, table, queryKey, targetDb string, shard []string) (*config.Job, error) {
	db := ""
	var e error
	topic := ""
	if strings.HasPrefix(dsn, "mongodb://") {
		info, e := connstring.Parse(dsn)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		db = info.Database
		topic = "mongoconnector." + db + "." + table
	} else {
		_, db, e = mysqlx.ParseMySQLDatabase(dsn)
		if e != nil {
			logx.Error(e)
			return nil, e
		}
		topic = "canal" + "_" + db + "_" + table

	}
	job := config.Job{
		ID: id,
	}
	job.Source.Type = sourceType
	job.Source.Table = table
	job.Source.Dsn = dsn
	// job.Source.QueryKey = req.Source.QueryKey[k]
	job.Source.QueryKey = queryKey
	job.Source.Topic = topic

	job.Target.Type = config.TYPE_CLICKHOUSE
	job.Target.Shards = strx.DeepSplit(shard, ",")
	job.Target.ChProxy = strings.Split(shard[0], ",")[0]
	job.Target.Db = targetDb
	job.Target.Table = table
	return &job, nil
}

func getHistoryJobs(dmModel *model.DmModel) ([]config2.Job, error) {
	dms, err := dmModel.All()
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	jobs := make([]config2.Job, 0, len(dms))
	for _, v := range dms {
		job := config2.Job{
			ID: strconv.Itoa(v.ID),
		}
		s, err := strx.DecryptDsn(v.TargetShards)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		shards := new([]string)
		err = json.Unmarshal([]byte(s), shards)
		if err != nil {
			logx.Error(err)
			return nil, err
		}
		dsn, err := strx.DecryptDsn(v.SourceDsn)
		if err != nil {
			logx.Error(err)
			continue
		}
		job.Source.Type = v.SourceType
		job.Source.Table = v.SourceTable
		job.Source.Dsn = dsn
		job.Source.QueryKey = v.SourceQueryKey

		job.Target.Table = v.TargetTable
		job.Target.DB = v.TargetDB
		job.Target.ChProxy = v.TargetChProxy
		job.Target.Type = v.TargetType
		job.Target.Shards = strx.DeepSplit(*shards, ",")
		jobs = append(jobs, job)
	}
	return jobs, nil
}
