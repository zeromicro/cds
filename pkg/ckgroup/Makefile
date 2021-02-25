run:
	docker-compose -f docker/docker-compose.yaml up -d
	sleep 5s
	make ckconn
	make shardconn
	make ckgroup
	docker-compose -f docker/docker-compose.yaml down


ckconn:
	go test -v -tags=integration -run='ckConn_*'

shardconn:
	go test -v -tags=integration -run='shardConn_*'

ckgroup:
	go test -v -tags=integration -run='dbGroup_*'

env_up:
	docker-compose -f docker/docker-compose.yaml up -d

env_down:
	docker-compose -f docker/docker-compose.yaml down
