一、低延迟保证
1、Kafka优化
    生产者配置：
        acks=1：平衡可靠性和延迟，主节点确认即发送成功。
        linger.ms=20 和 batch.size=16384：批量发送减少网络开销。
        启用压缩（如 compression.type=snappy），减少数据传输量。
    消费者配置：
        max.poll.records=500：单次拉取更多记录，减少轮询次数。
        fetch.min.bytes=1MB：等待足够数据再拉取，减少小批量请求。

    Topic分区策略：
        分区数设置为消费者数量的倍数，确保负载均衡。
        按 player_id 哈希分区，相同玩家的数据集中在同一分区。
2、Flink作业优化
    并行度调整：
        Source/Sink并行度与Kafka分区数一致，避免资源闲置。
        窗口算子根据数据吞吐量动态调整并行度（如使用 setParallelism(8)）。
     状态管理：
        使用 RocksDB状态后端，支持大状态和增量Checkpoint。
        配置状态TTL（如 StateTtlConfig），自动清理过期状态。
    水印与延迟处理：
        设置合理的水印间隔（setAutoWatermarkInterval(100)），及时推进事件时间。
        允许延迟数据（.allowedLateness(Time.minutes(5))）并输出到侧流。

3、写入优化（Doris）
    批量提交：
        通过Doris Connector设置 sink.batch.size=2000 和 sink.batch.interval=5s，减少高频小批次写入。
    表设计优化：
        按时间分区（PARTITION BY RANGE），便于数据管理和查询加速。
        分桶键选择高频过滤字段（如 player_id），提升查询性能。

二、高可靠性保证
1、端到端Exactly-Once语义
    Kafka事务支持：
        生产者启用幂等性（enable.idempotence=true）和事务（transactional.id）。
        Flink Kafka Consumer配合Checkpoint提交offset，保证消费与状态的一致性。
    Flink Checkpoint机制：
        设置Checkpoint间隔为1分钟（env.enableCheckpointing(60000)）。
        使用 RocksDB增量Checkpoint，减少每次Checkpoint的数据量。
        配置Checkpoint超时时间（checkpointTimeout=10min），避免因偶发延迟导致失败。

2、Doris事务写入：
    通过Stream Load事务接口，确保批次写入的原子性。
    失败时依赖Flink的重试机制（sink.max-retries=3）自动恢复。

3、故障恢复与监控
    监控体系：
        使用Prometheus监控Flink的背压指标（inPoolUsage、outPoolUsage）。
        通过Doris的 SHOW LOAD 监控数据导入状态。
    告警策略：
        Checkpoint失败率超过10%触发告警。
        Doris写入延迟超过30秒触发扩容通知

三、低延迟 高可靠需要平衡考虑