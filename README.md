# data-sync
基于canal的client-adapter开发，抽离原有项目的rdb插件及支持rocketmq
同步代码canal-adapter + CanalRocketMQClientExample订阅MQ + strategy

## 功能模块
+ 全量数据ETL，DB -> DB
+ 增量数据同步， DB -> Canal -> MQ -> Client

## todo:
1、配置中心
2、增量数据
3、ddl处理