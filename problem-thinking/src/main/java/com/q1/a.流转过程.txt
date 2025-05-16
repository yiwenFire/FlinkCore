[游戏客户端/服务端]
  │
  ▼ (HTTP/WebSocket/TCP)
[数据采集层]
  │
  ▼ (JSON/Protobuf)
[Kafka Producer]
  │
  ▼
[Kafka Cluster]
  ├── online_events（玩家登录/退出事件）
  ├── payment_events（付费事件）
  └── ...
  │
  ▼
[实时计算框架]
  │
  ▼
[计算结果存储]
  ├── Doirs（OLAP分析/实时看板）
  └── HBase（明细存储）
  │
  ▼
[可视化/告警]
  ├── Grafana
  └── Prometheus AlertManager



数据流转过程：
    数据采集：客户端/服务端发送玩家行为事件（登录、退出、付费等）到数据采集层
    数据标准化：采集层对数据进行清洗、格式标准化（JSON Schema验证）
    Kafka 生产：通过分区策略（按玩家ID哈希）写入对应Topic
    实时消费：计算框架Flink消费Kafka数据，进行窗口聚合计算
    结果存储：计算结果同时写入多个存储系统
    可视化：通过BI工具展示实时仪表盘，异常指标触发告警

