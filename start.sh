#!/bin/bash

version="v1.0.0"

echo "
              ___  ____   ___
             / __)(  _ \ / __)
            ( (__  )(_) )\__ \\
             \___)(____/ (___/    ${version}
==============================================
"

echo "==============compile=================="
docker run -it --rm -e GOPROXY="https://goproxy.io" -e GO11MODULE=ON -v ${PWD}:/cds -w /cds golang:1.14 bash /cds/docker/build.sh

echo "==============安装数据库模块=================="
docker-compose -f ./docker/databases.yml up -d

echo "=============安装数据桥接模块================="
docker-compose -f ./docker/incr_sync.yml up -d

echo "=============安装上层应用模块================="
docker-compose -f ./docker/apps.yml up -d

cd docker/init
sh ./init.sh
echo "
===========安装完成，以下是登陆信息===============
  链接：http://localhost:3414
  用户名：admin@email.com
  密码：123456
"
