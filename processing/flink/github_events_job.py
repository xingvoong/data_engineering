"""
Flink streaming job: GitHub event aggregation + stale issue detection.

Two pipelines in one job:
  1. Event aggregates — 5-minute tumbling windows, count events per repo/type
     → writes to ClickHouse: github_pulse.event_aggregates
  2. Stale issue detection — reads IssuesEvent from Kafka, checks if the
     issue has been open longer than SLA_DAYS with no close action
     → writes to ClickHouse: github_pulse.stale_issue_alerts

Run inside the Flink container:
  docker exec github_pulse_flink_jobmanager \
    python /opt/flink/jobs/github_events_job.py

Requirements (installed in Flink image via Dockerfile or pip):
  apache-flink==1.18.0  clickhouse-driver==0.2.7
"""

import json
import os
import uuid
from datetime import datetime, timezone

from pyflink.common import SimpleStringSchema, Time, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    ReduceFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GITHUB_EVENTS", "github-events")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "github_pulse")
SLA_DAYS = int(os.getenv("STALE_ISSUE_SLA_DAYS", "7"))


class CountReducer(ReduceFunction):
    def reduce(self, a, b):
        return (a[0], a[1], a[2], a[3] + b[3])


class AggregateWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        window = context.window()
        window_start = datetime.fromtimestamp(window.start / 1000, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end / 1000, tz=timezone.utc)
        repo_name, event_type = key
        count = sum(e[3] for e in elements)
        yield (window_start.isoformat(), window_end.isoformat(), repo_name, event_type, count)


class ClickHouseSink:
    """Write records to ClickHouse using the native driver."""

    def __init__(self, table: str, columns: list[str]):
        self.table = table
        self.columns = columns
        self._client = None

    def _get_client(self):
        if self._client is None:
            from clickhouse_driver import Client
            self._client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
        return self._client

    def write(self, rows: list[tuple]):
        cols = ", ".join(self.columns)
        self._get_client().execute(
            f"INSERT INTO {self.table} ({cols}) VALUES",
            rows,
        )


def parse_event(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def detect_stale(event: dict) -> dict | None:
    """Flag IssueEvents where an issue has been open longer than SLA_DAYS."""
    if event.get("type") != "IssuesEvent":
        return None
    if event.get("payload_action") == "closed":
        return None

    created_at_str = event.get("created_at", "")
    try:
        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    except ValueError:
        return None

    days_open = (datetime.now(timezone.utc) - created_at).days
    if days_open < SLA_DAYS:
        return None

    return {
        "alert_id": str(uuid.uuid4()),
        "repo_name": event.get("repo_name", "unknown"),
        "issue_number": 0,
        "issue_title": "(detected via event stream)",
        "author_login": event.get("actor_login", "unknown"),
        "created_at": created_at.isoformat(),
        "days_open": days_open,
        "sla_threshold": SLA_DAYS,
    }


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("github-pulse-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="github-events-kafka",
    )

    parsed = raw_stream.map(parse_event).filter(lambda e: e is not None)

    # ── Pipeline 1: 5-minute event aggregates ─────────────────────
    agg_sink = ClickHouseSink(
        "event_aggregates",
        ["window_start", "window_end", "repo_name", "event_type", "event_count"],
    )

    (
        parsed
        .map(lambda e: (e["repo_name"], e["type"], e["created_at"], 1),
             output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()]))
        .key_by(lambda e: (e[0], e[1]))
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .reduce(CountReducer(), AggregateWindowFunction())
        .map(lambda r: agg_sink.write([r]))
    )

    # ── Pipeline 2: stale issue detection ─────────────────────────
    stale_sink = ClickHouseSink(
        "stale_issue_alerts",
        ["alert_id", "repo_name", "issue_number", "issue_title",
         "author_login", "created_at", "days_open", "sla_threshold"],
    )

    (
        parsed
        .map(detect_stale)
        .filter(lambda a: a is not None)
        .map(lambda a: stale_sink.write([(
            a["alert_id"], a["repo_name"], a["issue_number"], a["issue_title"],
            a["author_login"], a["created_at"], a["days_open"], a["sla_threshold"],
        )]))
    )

    env.execute("github-pulse-streaming")


if __name__ == "__main__":
    main()
