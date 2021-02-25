package canalx

const canalxTmpl = `
""#################################################
## mysql serverId , v1.0.26+ will autoGen
# canal.instance.mysql.slaveId=0

# enable gtid use true/false
canal.instance.gtidon=false

# position info
canal.instance.master.address={{.Addr}}
canal.instance.master.journal.name=
canal.instance.master.position=
canal.instance.master.timestamp=
canal.instance.master.gtid=

# rds oss binlog
canal.instance.rds.accesskey=
canal.instance.rds.secretkey=
canal.instance.rds.instanceId=

# table meta tsdb info
canal.instance.tsdb.enable=true
#canal.instance.tsdb.url=jdbc:mysql://127.0.0.1:3306/canal_tsdb
#canal.instance.tsdb.dbUsername=canal
#canal.instance.tsdb.dbPassword=canal

# username/password
canal.instance.dbUsername={{.User}}
canal.instance.dbPassword={{.Password}}
canal.instance.connectionCharset = UTF-8
canal.instance.enableDruid=false

# table regex
canal.instance.filter.regex={{.Dbname}}.{{.TableName}}
canal.instance.filter.black.regex=

# mq config
canal.mq.topic={{.Topic}}
canal.mq.partition=0
#################################################

""
`
