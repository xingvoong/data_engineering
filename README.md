# GitHub Pulse — Open Source Health Monitor

Maintaining an open source project is mostly flying blind.

You get a flood of issues and PRs, no clear picture of what's urgent, and no signal on whether your community is growing or slowly dying. GitHub's built-in UI gives you raw counts — it doesn't tell you anything actionable.

GitHub Pulse fixes that. It's a data platform for open source maintainers and engineering teams that own or depend on open source libraries. It turns raw GitHub activity into operational intelligence: what needs attention, what's trending, and what's at risk.

**GitHub tells you what happened. GitHub Pulse tells you what to do about it.**

---

## What It Does

- **Detects stale issues** — surfaces issues open 30+ days with no maintainer response, so you know where to focus
- **Tracks contributor retention** — are new contributors coming back, or is everyone a one-time drive-by?
- **Measures PR velocity** — how long from open to merge, and is it getting slower over time?
- **Flags dependency risk** — repos your project depends on that are going quiet (no commits, piling issues)
- **Surfaces community health trends** — is engagement growing or declining over the last 90 days?

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│         GitHub REST API              GitHub Events API          │
└────────────────┬────────────────────────────┬───────────────────┘
                 │                            │
                 ▼                            ▼
         ┌──────────────┐           ┌──────────────────┐
         │     dlt      │           │  Kafka Producer  │
         │ (batch load) │           │ (event streaming)│
         └──────┬───────┘           └────────┬─────────┘
                │                            │
                ▼                            ▼
┌──────────────────────────────────────────────────────────────────┐
│                     RAW LAYER (Apache Iceberg on MinIO/S3)       │
│         repositories │ issues │ pull_requests │ github_events    │
└──────────────────────────────┬───────────────────────────────────┘
                                │                  ▲
                    ┌───────────┘          ┌───────┴────────┐
                    ▼                      │   Apache Flink  │
             ┌────────────┐               │ (stream process)│
             │    dbt     │               └───────┬─────────┘
             │(transform) │                       │
             └──────┬─────┘                       │
                    │                             │
                    ▼                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     CURATED LAYER                                │
│    repo_health_metrics │ contributor_retention │ issue_triage    │
│              event_aggregates (5-min windows)                    │
└────────────────────────────┬─────────────────────────────────────┘
                             │
           ┌─────────────────┼──────────────────┐
           ▼                 ▼                  ▼
    ┌────────────┐   ┌──────────────┐   ┌─────────────┐
    │ ClickHouse │   │    Feast     │   │   Qdrant    │
    │ (analytics)│   │ (features)   │   │  (vectors)  │
    └────────────┘   └──────────────┘   └─────────────┘
           │                 │                  │
           └─────────────────┼──────────────────┘
                             ▼
                    ┌─────────────────┐
                    │    Dagster      │
                    │ (orchestration) │
                    └─────────────────┘
```

---

## Stack

| Layer | Tool | Why |
|---|---|---|
| Batch ingestion | dlt | Lightweight, incremental by default, faster to iterate than Airbyte |
| Streaming ingest | Kafka + Python producer | Industry standard event backbone |
| Stream processing | Apache Flink | Low-latency stream processing for real-time issue detection |
| Table format | Apache Iceberg | The open table format — Snowflake, Databricks, and AWS all support it |
| Local storage | MinIO | S3-compatible, runs in Docker |
| Transformations | dbt | Curated health metrics and retention models |
| Analytics DB | ClickHouse | Fast OLAP, handles real-time event aggregates well |
| Feature store | Feast | Contributor churn features for ML models |
| Vector DB | Qdrant | Semantic search across repo issues and descriptions |
| Orchestration | Dagster | Asset-aware, code-first orchestration with daily schedule |
| Data quality | Great Expectations | Validates pipeline output before it reaches consumers |
| Local query | DuckDB | Fast local analytics during development |

---

## Project Phases

### Phase 1 — Batch Lakehouse

Ingest historical GitHub data and build the core health metrics. This is the foundation everything else builds on.

**What gets built:**
- dlt pipeline: repos, issues, pull requests (incremental via `updated_at`)
- Iceberg tables: raw layer on MinIO
- dbt models:
  - `repo_health_metrics` — composite score (0–100) based on stars, issue close rate, PR merge rate, recency
  - `contributor_retention` — tracks whether new contributors return within 90 days
  - `issue_resolution_stats` — SLA funnel: % of issues closed within 1 / 7 / 30 days
- Dagster: daily schedule ingesting and transforming at 06:00 UTC

**Skills demonstrated:** dlt, Iceberg, dbt, DuckDB, ClickHouse, Dagster

---

### Phase 2 — Real-Time Stale Issue Detection

Add a streaming layer that catches problems as they happen — not 24 hours later when the batch runs.

**What gets built:**
- Kafka producer: polls GitHub Events API every 60s, publishes to `github-events` topic
- Flink job: detects issues with no maintainer response within a configurable SLA window
- Sink: stale issue alerts written to ClickHouse in real time
- Dagster sensor: monitors Kafka consumer lag, alerts when lag exceeds threshold

**Why streaming matters here:** A stale issue caught in 5 minutes can be triaged before it gets buried. Catching it in 24 hours (batch) means it's already in a contributor's bad experience.

**Skills demonstrated:** Kafka, Apache Flink, stream windowing, ClickHouse real-time ingestion

---

### Phase 3 — Contributor Churn Prediction

Predict which contributors are about to disengage so maintainers can reach out before they're gone.

**What gets built:**
- Feast feature views: contributor activity features (PR frequency, comment rate, days since last contribution)
- Feature materialization: runs downstream of dbt, feeds a churn likelihood score
- Embedding pipeline: vectorizes issue text to find semantically similar unresolved issues
- Qdrant collection: enables "find issues similar to this one" for faster triage

**Skills demonstrated:** Feast, vector embeddings, Qdrant, ML/DE boundary, feature serving

---

## Quickstart

**Prerequisites:** Docker, Docker Compose, Python 3.11+, GitHub personal access token

```bash
# 1. Clone and configure
git clone https://github.com/xingvoong/data_engineering
cd data_engineering
cp .env.example .env
# set GITHUB_TOKEN in .env

# 2. Start all services
make up

# 3. Run batch ingestion
make ingest-batch

# 4. Run dbt transformations
make dbt-run

# 5. Start Dagster UI
make dagster-dev
# open http://localhost:3000
```

---

## Project Structure

```
data_engineering/
├── docker-compose.yml          # MinIO, ClickHouse, Kafka, Flink, Qdrant
├── .env.example                # Environment variable template
├── Makefile                    # Commands for every layer
├── ingestion/
│   ├── batch/                  # dlt pipelines (repos, issues, PRs)
│   └── streaming/              # Kafka producer (GitHub Events API)
├── processing/
│   ├── flink/                  # PyFlink streaming jobs
│   └── dbt/                    # Staging + mart models, tests
├── storage/
│   └── iceberg/                # PyIceberg catalog config, schema definitions
├── features/
│   ├── feast/                  # Feature store config and feature views
│   └── vectors/                # Embedding pipeline, Qdrant upsert
├── orchestration/
│   └── dagster/                # Assets, jobs, schedules, sensors
├── analytics/
│   └── queries/                # Example ClickHouse queries
└── tests/                      # Unit tests for pipeline and dbt SQL logic
```
