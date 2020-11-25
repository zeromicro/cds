PROJECT="CDS"

default:
	go run dm/cmd/sync/dm.go -f dm/cmd/sync/etc/dm.json &> logs/dm.log & go run galaxy/cmd/api/galaxy.go -f galaxy/etc/galaxy-api.json &> logs/galaxy.log

build:
	go clean
	go build -o dm dm/cmd/sync/dm.go
	go build -o galaxy galaxy/cmd/api/galaxy.go

start-dm:
	dm/dm -f dm/cmd/sync/etc/dm.json &> logs/dm.log

start-galaxy:
	galaxy/galaxy -f galaxy/etc/galaxy-api.json &> logs/galaxy.log

