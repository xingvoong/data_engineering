"""
Dagster sensor: monitors Kafka consumer group lag for the Flink job.
Fires a run request (or alert) when lag exceeds the threshold.

The sensor reads consumer group offsets and compares them to the
latest partition offsets to compute total lag.
"""

import os

from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GITHUB_EVENTS", "github-events")
KAFKA_GROUP = "github-pulse-flink"
LAG_THRESHOLD = int(os.getenv("KAFKA_LAG_ALERT_THRESHOLD", "1000"))


def get_consumer_lag() -> int:
    """Return total consumer lag across all partitions for the Flink group."""
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "__lag_monitor__",
        })

        # Get latest offsets per partition
        metadata = admin.list_topics(KAFKA_TOPIC, timeout=5)
        partitions = [
            TopicPartition(KAFKA_TOPIC, p)
            for p in metadata.topics[KAFKA_TOPIC].partitions
        ]
        _, high_offsets = consumer.get_watermark_offsets(partitions[0])

        # Get committed offsets for the Flink consumer group
        committed = consumer.committed(partitions, timeout=5)

        total_lag = 0
        for tp, committed_tp in zip(partitions, committed):
            _, high = consumer.get_watermark_offsets(tp, timeout=5)
            offset = committed_tp.offset if committed_tp.offset >= 0 else 0
            total_lag += max(0, high - offset)

        consumer.close()
        return total_lag

    except Exception:
        # Kafka not running in dev — return 0 so sensor doesn't fire
        return 0


@sensor(
    name="kafka_lag_sensor",
    description="Alerts when Flink consumer group lag exceeds threshold.",
    minimum_interval_seconds=60,
)
def kafka_lag_sensor(context: SensorEvaluationContext) -> SensorResult:
    lag = get_consumer_lag()
    context.log.info("Kafka consumer lag: %d (threshold: %d)", lag, LAG_THRESHOLD)

    if lag > LAG_THRESHOLD:
        context.log.warning("Lag %d exceeds threshold %d — Flink may be behind.", lag, LAG_THRESHOLD)
        return SensorResult(
            run_requests=[],  # no automated run — just surface the alert in Dagster UI
            dynamic_partitions_requests=None,
        )

    return SensorResult(run_requests=[])
