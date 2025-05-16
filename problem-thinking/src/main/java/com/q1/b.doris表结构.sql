CREATE TABLE online_stats (
  player_id VARCHAR(64) NOT NULL,
  duration BIGINT COMMENT '在线时长(秒)',
  window_end DATETIME COMMENT '窗口结束时间'
) ENGINE=OLAP
DUPLICATE KEY(player_id)
PARTITION BY RANGE(window_end)()
DISTRIBUTED BY HASH(player_id) BUCKETS 10
PROPERTIES (
  "replication_num" = "3",
  "storage_format" = "V2"
);


CREATE TABLE payment_stats (
  player_id VARCHAR(64) NOT NULL,
  total_amount DOUBLE COMMENT '总金额',
  window_end DATETIME COMMENT '窗口结束时间'
) ENGINE=OLAP
DUPLICATE KEY(player_id)
PARTITION BY RANGE(window_end)()
DISTRIBUTED BY HASH(player_id) BUCKETS 10
PROPERTIES (
  "replication_num" = "3",
  "storage_format" = "V2"
);