# GitHub Pulse — Unified Data Engineering Platform

A production-style data engineering portfolio project. One platform, three skill clusters: batch pipelines, real-time streaming, and AI/ML feature pipelines. Built on the tools that appear in 2026 job postings.

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
│    repo_health_metrics │ developer_activity │ issue_resolution   │
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
| Stream processing | Apache Flink | Replaces Spark Streaming for low-latency work |
| Table format | Apache Iceberg | The open table format — Snowflake, Databricks, and AWS all support it |
| Local storage | MinIO | S3-compatible, runs in Docker |
| Transformations | dbt | Standard for curated layer, required in most DE job postings |
| Analytics DB | ClickHouse | Fast OLAP, handles real-time event aggregates well |
| Feature store | Feast | ML feature serving, bridges DE and ML teams |
| Vector DB | Qdrant | Semantic search on repo descriptions via embeddings |
| Orchestration | Dagster | Asset-aware, code-first — signals you know Airflow isn't the only option |
| Data quality | Great Expectations | Production maturity signal |
| Local query | DuckDB | Fast local analytics, used in dlt pipeline |

---

## Project Phases

### Phase 1 — Batch Lakehouse

Build the foundation. Ingest GitHub data via REST API using dlt, land it in Iceberg on MinIO, transform with dbt, query with ClickHouse and DuckDB.

**Deliverables:**
- dlt pipeline: repos, issues, pull requests, commits (incremental via `updated_at`)
- Iceberg tables: raw layer on MinIO
- dbt models: staging + 3 mart models (repo health, developer activity, issue resolution)
- dbt tests: not null, unique, accepted values
- ClickHouse: curated tables queryable via SQL
- Dagster: daily schedule running the full batch pipeline

**Skills demonstrated:** dlt, Iceberg, dbt, DuckDB, ClickHouse, Dagster

---

### Phase 2 — Real-Time Streaming Pipeline

Add the streaming layer on top of the batch foundation. Same Iceberg storage, same curated layer — now with a live event feed.

**Deliverables:**
- Kafka producer: polls GitHub Events API every 60s, publishes to `github-events` topic
- Flink job: consumes events, 5-minute tumbling window aggregations by repo
- Sink: aggregated results written to ClickHouse `event_aggregates` table
- Dagster sensor: monitors Kafka consumer lag, alerts when lag exceeds threshold
- Docker Compose: Kafka + Zookeeper + Flink JobManager + TaskManager all wired up

**Skills demonstrated:** Kafka, Apache Flink, stream windowing, ClickHouse real-time ingestion

---

### Phase 3 — AI/ML Feature Pipeline

Extend the platform into ML territory. Compute batch features for a feature store, generate embeddings for semantic search.

**Deliverables:**
- Feast feature views: repo health scores, developer metrics (sourced from dbt marts)
- Feature materialization: Dagster asset triggers Feast materialization on dbt completion
- Embedding pipeline: `sentence-transformers` generates vectors from repo descriptions
- Qdrant collection: `github-repos` with payload (repo_id, name, description, stars, language)
- Example query: "find repos similar to Apache Kafka" via vector similarity search
- Dagster: feature pipeline runs downstream of batch pipeline

**Skills demonstrated:** Feast, vector embeddings, Qdrant, ML/DE boundary, feature serving

---

## Quickstart

**Prerequisites:** Docker, Docker Compose, Python 3.11+, GitHub personal access token

```bash
# 1. Clone and configure
git clone <repo>
cd data_engineering
cp .env.example .env
# add your GITHUB_TOKEN to .env

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
├── docker-compose.yml          # All services: Kafka, Flink, ClickHouse, MinIO, Qdrant, Dagster
├── .env.example
├── Makefile
├── ingestion/
│   ├── batch/                  # dlt pipelines (repos, issues, PRs, commits)
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
└── quality/
    └── expectations/           # Great Expectations suites
```

---

## Why This Project

Most DE portfolios are either tutorial replicas (single pipeline, CSV input, no tests) or too narrow (batch only, no streaming). This project covers what hiring managers actually score:

1. **End-to-end ownership** — from raw API to analytical queries to ML features
2. **Streaming is not optional** — 72% of companies now run streaming for critical operations
3. **Iceberg is the table format** — the open lakehouse debate is settled
4. **AI/ML boundary** — DEs are now expected to own feature pipelines, not just ETL
5. **Production signals** — data quality checks, failure handling, observability, Docker

One project. Three skill clusters. Everything on the resume is in the repo.
