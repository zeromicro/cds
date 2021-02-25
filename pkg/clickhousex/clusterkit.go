package clickhousex

import (
	"time"
)

func ExecWithRetry(dsn string, retry int, f func(dsn string) error) error {
	e := f(dsn)
	if e == nil {
		return nil
	}
	if retry == 0 {
		return e
	}
	e = nil
	for i := 0; i < retry; i++ {
		time.Sleep(time.Second)
		e = f(dsn)
		if e == nil {
			return nil
		}
	}
	return e
}

func ExecClusterAllReplicas(shards [][]string, retry int, f func(dsn string) error) error {
	for _, shard := range shards {
		for _, replica := range shard {
			e := ExecWithRetry(replica, retry, f)
			if e != nil {
				return e
			}
		}
	}
	return nil
}

func ExecClusterEachShards(shards [][]string, retry int, f func(dsn string) error) error {
	for _, shard := range shards {
		fail := []error{}
		for _, replica := range shard {
			e := ExecWithRetry(replica, retry, f)
			if e != nil {
				fail = append(fail, e)
				continue
			}
			break
		}
		if len(fail) > 0 && len(fail) == len(shard) {
			return fail[0]
		}
	}
	return nil
}

func ExecClusterEachShardsAll(shards [][]string, retry int, f func(dsn string) error) error {
	for _, shard := range shards {
		fail := []error{}
		for _, replica := range shard {
			e := ExecWithRetry(replica, retry, f)
			if e != nil {
				fail = append(fail, e)
				continue
			}
		}
		if len(fail) > 0 && len(fail) == len(shard) {
			return fail[0]
		}
	}
	return nil
}

func ExecClusterAnyShard(shards [][]string, retry int, f func(dsn string) error) error {
	var failed error
	for _, shard := range shards {
		fail := []error{}
		for _, replica := range shard {
			e := ExecWithRetry(replica, retry, f)
			if e != nil {
				fail = append(fail, e)
				continue
			}
			return nil
		}
		if len(fail) == 0 {
			return nil
		}
		failed = fail[0]
	}
	return failed
}
