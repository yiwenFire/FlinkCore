package com.q1;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public class GameDataProcessor {

    public static void main(String[] args) {
        // ===== 步骤1: 初始化Flink执行环境 =====
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000); // 开启60秒间隔的Checkpoint


        // ===== 步骤2: 定义数据源 (Kafka输入) =====
        // 在线事件流
        DataStream<OnlineEvent> onlineStream = env
                .fromSource(
                        kafkaOnlineSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "OnlineEvent Source"
                );

        // 付费事件流
        DataStream<PaymentEvent> paymentStream = env
                .fromSource(
                        kafkaPaymentSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "PaymentEvent Source"
                );

        // ===== 步骤3: 核心计算逻辑 =====
        // --- 3.1 计算玩家在线时长 (会话窗口) ---
        DataStream<OnlineStats> onlineStats = onlineStream
                .keyBy(event -> event.playerId)
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .process(new CalculateOnlineDuration()) // 自定义窗口处理函数
                .map(stats -> formatOnlineStats(stats)); // 转换为输出格式

        // --- 3.2 计算玩家付费总额 (滚动窗口) ---
        DataStream<PaymentStats> paymentStats = paymentStream
                .keyBy(event -> event.playerId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SumPaymentAmount()) // 聚合函数
                .map(stats -> formatPaymentStats(stats));

        // ===== 步骤4: 结果写入Doris =====
        // 配置Doris Sink
        DorisSink<OnlineStats> onlineSink = buildDorisSink("online_stats");
        DorisSink<PaymentStats> paymentSink = buildDorisSink("payment_stats");

        // 写入数据
        onlineStats.sinkTo(onlineSink);
        paymentStats.sinkTo(paymentSink);

        // ===== 步骤5: 执行任务 =====
        env.execute("Real-time Game Analytics Job");
    }


    // ===== 工具方法 =====
    private DorisSink<T> buildDorisSink(String tableName) {
        return DorisSink.sink(
                DorisOptions.builder()
                        .setFenodes("doris-fe:8030")
                        .setTableIdentifier("game_db." + tableName)
                        .build(),
                DorisExecutionOptions.builder()
                        .setBatchSize(2000)
                        .build(),
                (record, context) -> serializeToDorisFormat(record) // 序列化逻辑
        );
    }

    // ===== 数据模型 =====
    class OnlineEvent { String playerId; long timestamp; String eventType; }
    class PaymentEvent { String playerId; double amount; long timestamp; }
    class OnlineStats { String playerId; long duration; String windowEnd; }
    class PaymentStats { String playerId; double totalAmount; String windowEnd; }


}
