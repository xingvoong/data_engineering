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
│                  RAW LAYER (DuckDB / Iceberg on MinIO)           │
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
│  repo_health_metrics │ developer_activity │ issue_resolution     │
│              event_aggregates (5-min windows)       [Phase 2]    │
└────────────────────────────┬─────────────────────────────────────┘
                             │
           ┌─────────────────┼──────────────────┐
           ▼                 ▼                  ▼
    ┌────────────┐   ┌──────────────┐   ┌─────────────┐
    │ ClickHouse │   │    Feast     │   │   Qdrant    │
    │ (analytics)│   │ (features)   │   │  (vectors)  │
    │ [Phase 2]  │   │ [Phase 3]    │   │ [Phase 3]   │
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
| Local storage | DuckDB (dev) / MinIO (prod) | DuckDB for fast local iteration; MinIO for S3-compatible Iceberg storage |
| Transformations | dbt | Curated health metrics and retention models |
| Analytics DB | ClickHouse | Fast OLAP, handles real-time event aggregates well |
| Feature store | Feast | Contributor churn features for ML models |
| Vector DB | Qdrant | Semantic search across repo issues and descriptions |
| Orchestration | Dagster | Asset-aware, code-first orchestration with daily schedule |
| Data quality | dbt tests + Great Expectations | 19 tests validating shape, uniqueness, and value ranges |

---

## Project Phases

### Phase 1 — Batch Lakehouse ✅

Ingest historical GitHub data and build the core health metrics.

```mermaid
flowchart LR
    A([GitHub REST API]) -->|repos / issues / PRs| B[dlt\nincremental load]
    B -->|parquet| C[(DuckDB\ngithub_raw)]

    C --> D[stg_repos]
    C --> E[stg_issues]
    C --> F[stg_pull_requests]

    D & E & F --> G[repo_health_metrics\n0–100 score]
    D & E & F --> H[developer_activity\ncontribution score]
    E --> I[issue_resolution_stats\nSLA funnel]

    G & H & I --> J[(DuckDB\nmain schema)]

    K([Dagster]) -->|daily 06:00 UTC| B
    K -->|triggers| G
    K -->|triggers| H
    K -->|triggers| I
```

**What's running:**
- dlt pipeline ingesting incrementally via `updated_at`:
  - 2,681 repositories across 4 data engineering topics
  - 1,383 issues from 5 tracked projects (Apache Iceberg, Flink, Kafka, dbt, Dagster)
  - 25,000 pull requests
- dbt models (19 tests, all passing):
  - `stg_repos`, `stg_issues`, `stg_pull_requests` — clean staging views
  - `repo_health_metrics` — composite score (0–100): stars + issue close rate + PR merge rate + recency
  - `developer_activity` — contribution scores per developer per repo
  - `issue_resolution_stats` — SLA funnel: % of issues closed within 1 / 7 / 30 days
- Dagster assets wired up with daily 06:00 UTC schedule

**Sample output:**

| Repo | Stars | Health Score | PR Merge Rate |
|---|---|---|---|
| dagster-io/dagster | 16,092 | 64.4 | 70.4% |
| apache/superset | 74,593 | 50.0 | — |
| apache/airflow | 46,700 | 49.8 | — |

Apache Iceberg issue SLA: 69% close rate, median 195 days to close, only 20% resolved within 7 days.

**Skills demonstrated:** dlt, dbt, DuckDB, Dagster, incremental loading, data quality testing

---

### Phase 2 — Real-Time Stale Issue Detection ✅

Add a streaming layer that catches problems as they happen — not 24 hours later when the batch runs.

```mermaid
flowchart LR
    A([GitHub Events API]) -->|poll every 60s| B[Kafka Producer]
    B -->|github-events topic| C[(Kafka\n3 partitions)]

    C --> D[Flink Job]

    D -->|5-min tumbling window\ncounts per repo/type| E[(ClickHouse\nevent_aggregates)]
    D -->|issue open > SLA days\nno close action| F[(ClickHouse\nstale_issue_alerts)]

    G([Dagster Sensor]) -->|check lag every 60s| C
    G -->|lag > 1000 → alert| H[Dagster UI]
```

**What's running:**
- Kafka producer polling GitHub Events API every 60s, deduplicating in-memory, publishing to `github-events` (3 partitions)
- Flink job with two pipelines:
  - 5-minute tumbling window aggregations (event count per repo + type) → `event_aggregates`
  - Stale issue detection (IssuesEvents open > SLA threshold) → `stale_issue_alerts`
- ClickHouse tables for real-time query access
- Dagster sensor polling consumer group lag every 60s, surfaces alert in UI when lag > 1000

**Why streaming matters here:** A stale issue caught in 5 minutes can be triaged before it gets buried. Catching it in 24 hours (batch) means it's already in a contributor's bad experience.

**Skills demonstrated:** Kafka, Apache Flink, stream windowing, ClickHouse real-time ingestion, Dagster sensors

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

# 2. Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Run batch ingestion (writes to local DuckDB)
python ingestion/batch/github_pipeline.py

# 4. Run dbt transformations
cd processing/dbt
dbt deps
dbt run

# 5. Run tests
dbt test

# 6. Start Dagster UI  (optional)
cd ../..
dagster dev -f orchestration/dagster/__init__.py
# open http://localhost:3000

# 7. Start Docker services for full stack (MinIO + ClickHouse)
make up
```

---

## Project Structure

```
data_engineering/
├── docker-compose.yml          # MinIO, ClickHouse (Phase 2: + Kafka, Flink, Qdrant)
├── .env.example                # Environment variable template
├── Makefile                    # Commands for every layer
├── requirements.txt            # Python dependencies
├── ingestion/
│   ├── batch/                  # dlt pipeline (repos, issues, PRs) — Phase 1 ✅
│   └── streaming/              # Kafka producer (GitHub Events API) — Phase 2
├── processing/
│   ├── flink/                  # PyFlink streaming jobs — Phase 2
│   └── dbt/                    # Staging + mart models, tests — Phase 1 ✅
├── storage/
│   └── iceberg/                # PyIceberg catalog config, schema definitions
├── features/
│   ├── feast/                  # Feature store config and feature views — Phase 3
│   └── vectors/                # Embedding pipeline, Qdrant upsert — Phase 3
├── orchestration/
│   └── dagster/                # Assets, jobs, schedules, sensors — Phase 1 ✅
└── tests/                      # Unit tests for pipeline and dbt SQL logic — Phase 1 ✅
```
