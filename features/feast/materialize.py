"""
materialize.py — Bridge DuckDB → parquet → Feast online store.

Steps:
  1. Export contributor_churn_features from DuckDB to parquet.
  2. Call FeatureStore.materialize() to push features into the SQLite online store.

Usage:
    python features/feast/materialize.py
    python features/feast/materialize.py --start 2026-09-07 --end 2026-09-08
    python features/feast/materialize.py --duckdb-path /path/to/github_pulse.duckdb
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
from feast import FeatureStore

# Paths relative to this script's location
_SCRIPT_DIR = Path(__file__).parent
_REPO_ROOT = _SCRIPT_DIR.parents[1]  # data_engineering/
_PARQUET_DIR = _SCRIPT_DIR / "data" / "parquet" / "contributor_churn_features"
_DEFAULT_DUCKDB = _REPO_ROOT / "github_pulse.duckdb"


def export_to_parquet(duckdb_path: str) -> int:
    """Export contributor_churn_features to parquet. Returns row count."""
    _PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _PARQUET_DIR / "snapshot.parquet"

    print(f"Connecting to DuckDB: {duckdb_path}")
    con = duckdb.connect(str(duckdb_path), read_only=True)

    row_count = con.execute("SELECT COUNT(*) FROM contributor_churn_features").fetchone()[0]
    print(f"Exporting {row_count} rows to {out_path} ...")

    # Cast snapshot_date from DATE to TIMESTAMP — Feast's file offline store requires
    # a timestamp type (with tzinfo) for its timestamp_field, not a bare date.
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (snapshot_date),
                   CAST(snapshot_date AS TIMESTAMP) AS snapshot_date
            FROM contributor_churn_features
        ) TO '{out_path}' (FORMAT PARQUET)
    """)
    con.close()

    print(f"Export complete: {out_path}")
    return row_count


def materialize(start: datetime, end: datetime) -> None:
    """Push features from the parquet offline store into the SQLite online store."""
    store = FeatureStore(repo_path=str(_SCRIPT_DIR))

    print(f"Materializing features from {start.date()} to {end.date()} ...")
    store.materialize(start_date=start, end_date=end)
    print("Materialization complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export DuckDB mart to parquet and materialize into Feast online store."
    )
    parser.add_argument(
        "--duckdb-path",
        default=str(_DEFAULT_DUCKDB),
        help="Path to github_pulse.duckdb (default: project root)",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="ISO date for materialization window start (default: yesterday)",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="ISO date for materialization window end (default: today)",
    )
    args = parser.parse_args()

    end_dt = (
        datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        if args.end
        else datetime.now(tz=timezone.utc)
    )
    start_dt = (
        datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        if args.start
        else end_dt - timedelta(days=1)
    )

    row_count = export_to_parquet(args.duckdb_path)
    print(f"Parquet export: {row_count} rows")

    materialize(start=start_dt, end=end_dt)


if __name__ == "__main__":
    main()
