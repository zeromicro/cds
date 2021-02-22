clickhouse_version=${1-'20.8.12.2'}

sudo yum install -y yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/lts/x86_64
sudo yum install -y yum-plugin-versionlock
sudo yum versionlock clickhouse-server
content="0:clickhouse-server-$clickhouse_version-2.*
0:clickhouse-common-static-$clickhouse_version-2.*
0:clickhouse-client-$clickhouse_version-2.*
0:clickhouse-test-$clickhouse_version-2.*"
sudo echo "$content" | sudo tee /etc/yum/pluginconf.d/versionlock.list
sudo yum install clickhouse-server
sudo systemctl stop clickhouse-server
sudo systemctl daemon-reload
sudo systemctl start clickhouse-server
ps -ef | grep -v "grep" |grep "/usr/bin/clickhouse-server"
netstat -tunlp | grep -P "9000|8123"
