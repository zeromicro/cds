# ClickHouse Data System
基于 [go-zero](https://github.com/tal-tech/go-zero) 构建的 `ClickHouse` 的大数据数据同步、存储、计算系统。

![avatar](https://gitee.com/kevwan/static/raw/master/doc/images/cds/logo.png)

### 系统架构
下图展示了以clickhouse为存储和计算引擎的数仓架构。

![avatar](https://gitee.com/kevwan/static/raw/master/doc/images/cds/clickhouse_arch1.png)

### 数据同步设计：
该部分实现了从MySQL/MongoDB数据源自动实时同步数据到ClickHouse集群的功能。


![同步drawio](https://gitee.com/kevwan/static/raw/master/doc/images/cds/同步drawio.png)

#### 快速开始

[快速开始](doc/quickstart.md)

### todo list
 - 优化前端用户体验
 - 更详细的文档如部署方式
 - 建表方案
 - 注意事项

欢迎大家使用和 `star` 🤝

交流群

<img src="https://gitee.com/kevwan/static/raw/master/images/cds.png" alt="cds" width="310" />
