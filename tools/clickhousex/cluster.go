package clickhousex

type Cluster struct {
	QueryNode string
	Shards    [][]string
	Name      string
}
