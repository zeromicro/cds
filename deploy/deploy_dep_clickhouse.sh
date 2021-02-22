clickhouse_version=${1-'20.8.12.2'}
apt-get update
apt-get install apt-transport-https ca-certificates dirmngr
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4
echo "deb https://repo.clickhouse.tech/deb/lts/ main/" |tee \
  /etc/apt/sources.list.d/clickhouse.list
apt-get update
apt install  "clickhouse-server=$clickhouse_version" "clickhouse-client=$clickhouse_version" "clickhouse-common-static=$clickhouse_version"
service clickhouse-server stop
service clickhouse-server start
ps -ef | grep -v "grep" |grep "/usr/bin/clickhouse-server"
netstat -tunlp | grep -P "9000|8123"

#sudo apt-cache madison clickhouse-server

