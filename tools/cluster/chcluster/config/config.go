package config

type (
	// 总配置
	Config struct {
		ShardGroups []ShardGroupConfig
		QueryNode   string
	}
	// 分片node配置
	ShardGroupConfig struct {
		ShardNode   string
		RelicaNodes []string
	}
)
