# 回调中心

## 简介

回调中心（简称wecallback）是一个集中管理和监控消息队列（MQ）相关组件和中间件，旨在提供高效、安全、可靠且灵活的消息处理和回调服务。接入和使用回调中心，用户可以实现MQ节点、消息、消费者的统一管理和实时监控，确保消息传递的准确性和及时性。

回调中心凭借其统一的管理界面、实时监控、动态调整能力、多协议支持、消费者管理与跟踪、多种回调模式以及多MQ模型与场景支持等特点，为用户提供了一个高效、可靠且灵活的消息处理和回调服务平台。同时，一键部署和快速接入的特性也极大地降低了用户的使用门槛和成本。


### 支持特点
- 统一管理和实时监控
提供MQ节点、消息、消费者的集中管理界面，方便用户查看和管理各个组件的状态。
实时监控MQ的运行情况，包括消息发送量、接收量、堆积情况等关键指标。
- 动态调整消费者消费能力
支持根据业务需求动态调整消费者的消费速度，确保消息处理的及时性和资源的有效利用。
灵活设置消费者并行处理能力，以满足不同规模和复杂度的应用场景。
- 多协议支持
支持HTTP/RPC接口回调消费消息，满足各种异构系统之间的消息交互需求。
便于不同语言和框架的系统接入，提高系统的灵活性和可扩展性。
消- 费者管理与跟踪
提供消费者管理功能，包括创建、删除、修改消费者等操作。
跟踪消费者的消费进度和处理情况，便于用户及时发现问题并进行处理。
- 多种回调模式
支持回调接口模式，用户只需提供回调接口地址，平台将自动推送消息到指定接口。
提供SDK模式，用户可通过SDK接入平台，实现更加灵活和个性化的消息处理逻辑。
- 多MQ模型与场景支持
支持市面上常用的MQ模型（如Kafka、RabbitMQ、RocketMQ、Pulsar、ActiveMQ等）和场景。
满足各种业务需求，包括但不限于系统异步解耦、流量整形削峰、分布式事务处理、异步通信等。
- 一键部署与快速接入
提供一键部署功能，用户可轻松完成平台的安装和配置。
接入过程简单明了，支持快速集成到现有系统中，减少开发和运维成本。



## 软件架构
```text
|-- bin # 二进制文件目录
|-- cmd # 编译入口
|   `-- app
|-- deploy # 环境和部署相关目录
|   |-- docker-compose # docker-compose 容器编排目录
|   `-- kubernetes # k8s 编排配置目录
|-- config # 配置文件目录
|-- internal
|   `-- app
|       |-- command # 命令行功能模块
|       |   |-- handler
|       |   `-- script # 临时脚本
|       |-- component # 功能组件，如：db, redis 等
|       |-- config # 配置模型
|       |-- cron # 定时任务功能模块
|       |   `-- job
|       |-- common # 公共模块
|       |-- model # 数据库模型
|       |-- pkg # 功能类库
|       |-- dao # 数据处理层
|       |-- service # 业务逻辑层
|       |-- controller # 控制层
|       |-- router # 路由层
|       `-- transport
|           |-- grpc
|           |   |-- api # proto 文件目录
|           |   |-- handler # 控制层
|           |   `-- middleware # 中间件
|           `-- http
|               |-- api # swagger 文档
|               |-- handler # 控制层
|               |-- middleware # 中间件
|               `-- router # 路由
|-- logs # 日志目录
|-- pkg # 功能类库
`-- proto # 第三方 proto 文件目录
|-- test # 测试用例

```

软件架构说明

支持特性


## 设计文档


## 使用说明


## 常见问题