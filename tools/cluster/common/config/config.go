package config

type ShardGroupConfig struct {
	ShardNode    string
	ReplicaNodes []string
}

type Config struct {
	ShardGroups []ShardGroupConfig
	QueryNode   string
}
