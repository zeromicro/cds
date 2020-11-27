<p align="left">
  <img width ="200px" src="https://gitee.com/kevwan/static/raw/master/doc/images/cds/logo.png">
</p>

# Clickhouse Data System

基于 [go-zero](https://github.com/tal-tech/go-zero) 构建的 `ClickHouse` 的大数据数据同步、存储、计算系统。

## 🚀 特性

- 开箱即用，无需开发的同步系统
- 不只是同步，同时提供了 `ckgroup,tube...` 更加上层易用的 library 供开发者使用

## 📖介绍

从系统架构和同步设计上介绍 `CDS` ，并带开发者可以快速启动体验，启动一个同步任务。

### 系统架构

下图展示了以 `clickhouse` 为存储和计算引擎的数仓架构。

![avatar](https://gitee.com/kevwan/static/raw/master/doc/images/cds/clickhouse_arch1.png)

### 数据同步设计
该部分实现了从 `MySQL/MongoDB` 数据源自动实时同步数据到 `ClickHouse` 集群的功能。

![同步drawio](https://gitee.com/kevwan/static/raw/master/doc/images/cds/同步drawio.png)

接下来可以通过这个 [快速开始](doc/quickstart.md)，极速开始体验吧 :hammer: ​​

### Package

- [CkGroup](tools/ckgroup/README.md)：提供更上层，更易用 `clickhouse` 的API

### TODO

- [ ] 优化前端用户体验
- [ ] 更详细的文档如部署方式
- [ ] 建表方案优化
- [ ] 注意事项

## 🎡用户案例

欢迎大家使用和 `star` 🤝，也请添加您的项目在这里~~ :happy: ​

### 交流群

<img src="https://gitee.com/kevwan/static/raw/master/images/cds.jpg" alt="cds" width="310" />


