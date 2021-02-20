# data-sync
+ 基于canal的client-adapter开发，抽离原有项目的rdb插件及支持rocketmq
+ 同步代码canal-adapter + CanalRocketMQClientExample订阅MQ + strategy

## 功能模块
+ 全量数据ETL，DB -> DB
+ 增量数据同步， DB -> Canal -> MQ -> Client
+ 增加分布式ID插件leaf，用于ID生成

## todo:
+ 配置中心
+ 增量数据
+ ddl处理
+ 关于分布式ID需要将leaf-core上传至私服