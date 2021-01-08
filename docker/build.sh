set -e
cd /cds
echo "start go mod vendor"
go mod vendor
echo "end go mod vendor"
echo "start build rtu"
go build -o /cds/docker/build/rtu rtu/cmd/sync/rtu.go
echo "end build rtu"
echo "start build dm"
go build -o /cds/docker/build/dm dm/cmd/sync/dm.go
echo "end build dm"
echo "start build galaxy"
go build -o /cds/docker/build/galaxy galaxy/galaxy.go
echo "end build galaxy"
