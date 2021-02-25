cd docker
docker-compose up -d
echo "clickhouse 集群启动中\n"
echo "请等待.........\n"
sleep 5s

cd ../examples
go run .

echo "\n关闭 clickhouse cluster......... "
cd ../docker
docker-compose down
