PROJECT="CDS"

default:
	go run dm/cmd/sync/dm.go -f dm/cmd/sync/etc/dm.json &> logs/dm.log & go run galaxy/cmd/api/galaxy.go -f galaxy/etc/galaxy-api.json &> logs/galaxy.log

start-dm:
	dm/dm -f dm/cmd/sync/etc/dm.json &> logs/dm.log

start-galaxy:
	galaxy/galaxy -f galaxy/etc/galaxy-api.json &> logs/galaxy.log

build:
	go clean
	GO111MODULE=on GOARCH=amd64 go build -ldflags="-s -w"  -o docker/build/rtu      rtu/cmd/sync/rtu.go
	GO111MODULE=on GOARCH=amd64 go build -ldflags="-s -w"  -o docker/build/dm        dm/cmd/sync/dm.go
	GO111MODULE=on GOARCH=amd64 go build -ldflags="-s -w"  -o docker/build/galaxy    galaxy/cmd/api/galaxy.go


