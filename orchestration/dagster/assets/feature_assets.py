"""
Dagster software-defined assets for Phase 3: feature store and embeddings.

Asset dependency chain:
    github_dbt_assets
        ├── churn_features_parquet   (export dbt mart → parquet for Feast)
        │       └── feast_materialized_features  (Feast online store)
        └── issue_embeddings         (MiniLM embeddings → Qdrant)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

_PROJECT_ROOT = Path(__file__).parents[4]  # data_engineering/


@asset(
    group_name="features",
    deps=[AssetKey("github_dbt_assets")],
    compute_kind="duckdb",
    description="Exports contributor_churn_features dbt mart to parquet for Feast offline store.",
)
def churn_features_parquet(context: AssetExecutionContext):
    parquet_dir = _PROJECT_ROOT / "features" / "feast" / "data" / "parquet" / "contributor_churn_features"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    out_path = parquet_dir / "snapshot.parquet"

    db_path = _PROJECT_ROOT / "github_pulse.duckdb"
    context.log.info(f"Connecting to DuckDB at {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)
    row_count = con.execute("SELECT COUNT(*) FROM contributor_churn_features").fetchone()[0]
    context.log.info(f"Exporting {row_count} rows to {out_path}")

    con.execute(f"COPY contributor_churn_features TO '{out_path}' (FORMAT PARQUET)")
    con.close()

    context.add_output_metadata({
        "row_count": MetadataValue.int(row_count),
        "output_path": MetadataValue.path(str(out_path)),
    })


@asset(
    group_name="features",
    deps=[AssetKey("churn_features_parquet")],
    compute_kind="feast",
    description="Materializes contributor churn features into the Feast online store.",
)
def feast_materialized_features(context: AssetExecutionContext):
    from datetime import datetime, timedelta, timezone

    from feast import FeatureStore

    repo_path = _PROJECT_ROOT / "features" / "feast"
    context.log.info(f"Initializing Feast store at {repo_path}")
    store = FeatureStore(repo_path=str(repo_path))

    end_date = datetime.now(tz=timezone.utc)
    start_date = end_date - timedelta(days=1)

    context.log.info(f"Materializing features from {start_date.date()} to {end_date.date()}")
    store.materialize(start_date=start_date, end_date=end_date)
    context.log.info("Feast materialization complete.")

    context.add_output_metadata({
        "start_date": MetadataValue.text(str(start_date.date())),
        "end_date": MetadataValue.text(str(end_date.date())),
    })


@asset(
    group_name="features",
    deps=[AssetKey("github_dbt_assets")],
    compute_kind="qdrant",
    description="Embeds GitHub issue titles with MiniLM-L6-v2 and upserts vectors into Qdrant.",
)
def issue_embeddings(context: AssetExecutionContext):
    # Add project root to path so features.vectors.embed_issues is importable
    project_root_str = str(_PROJECT_ROOT)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    from features.vectors.embed_issues import main as embed_main

    db_path = str(_PROJECT_ROOT / "github_pulse.duckdb")
    qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")

    context.log.info(f"Starting embedding pipeline. DuckDB: {db_path}, Qdrant: {qdrant_url}")
    embed_main(duckdb_path=db_path, qdrant_url=qdrant_url)
    context.log.info("Issue embedding complete.")
