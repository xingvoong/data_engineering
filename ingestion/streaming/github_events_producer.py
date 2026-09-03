"""
GitHub Events Kafka producer.

Polls the GitHub Events API every 60 seconds and publishes new events
to the `github-events` Kafka topic. Deduplicates within the session
using a rolling set of seen event IDs.

Each message:
  key:   event_id (str)
  value: JSON with event_id, type, repo_name, actor_login, created_at, payload

Usage:
  python ingestion/streaming/github_events_producer.py
"""

import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GITHUB_EVENTS", "github-events")
POLL_INTERVAL = int(os.getenv("EVENTS_POLL_INTERVAL_SECONDS", "60"))

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# Rolling dedup window — keep last 5000 event IDs in memory
seen_ids: deque = deque(maxlen=5000)


def fetch_events() -> list[dict]:
    """Fetch the latest public GitHub events."""
    try:
        resp = requests.get(
            "https://api.github.com/events",
            headers=HEADERS,
            params={"per_page": 100},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.error("Failed to fetch events: %s", e)
        return []


def serialize(event: dict) -> dict:
    """Extract and flatten the fields we care about."""
    return {
        "event_id": event["id"],
        "type": event["type"],
        "repo_name": event["repo"]["name"],
        "actor_login": event["actor"]["login"],
        "created_at": event["created_at"],
        "is_public": event.get("public", True),
        "payload_action": event.get("payload", {}).get("action"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for event %s: %s", msg.key(), err)


def run():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    log.info("Producer started. Topic: %s | Poll interval: %ss", KAFKA_TOPIC, POLL_INTERVAL)

    while True:
        events = fetch_events()
        new_count = 0

        for event in events:
            event_id = event["id"]
            if event_id in seen_ids:
                continue

            seen_ids.append(event_id)
            payload = serialize(event)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event_id,
                value=json.dumps(payload),
                callback=delivery_report,
            )
            new_count += 1

        producer.flush()
        log.info("Published %d new events (total seen: %d)", new_count, len(seen_ids))
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
