"""
Tests for the DuckDB → parquet export step in features/feast/materialize.py.

Validates that the parquet output has the correct schema and column types.
Does NOT test Feast itself (that would require a running Feast registry).

Run with: pytest tests/test_feast_materialize.py -v
"""

import re
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

DBT_DIR = Path(__file__).parents[1] / "processing" / "dbt"


def load_sql(relative_path: str) -> str:
    """Read a dbt SQL file and strip Jinja refs for direct DuckDB execution."""
    path = DBT_DIR / "models" / relative_path
    sql = path.read_text()
    sql = re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"\1", sql)
    sql = re.sub(r"\{\{\s*source\('[^']+',\s*'(\w+)'\)\s*\}\}", r"\1", sql)
    return sql


@pytest.fixture
def db_with_churn_view():
    """
    In-memory DuckDB with staging tables and contributor_churn_features view.
    Uses minimal fixture data — one PR author and one issue author.
    """
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE stg_pull_requests (
            pr_id VARCHAR, pr_number INTEGER, repo_full_name VARCHAR, title VARCHAR,
            state VARCHAR, author_login VARCHAR, is_merged BOOLEAN,
            base_branch VARCHAR, head_branch VARCHAR,
            created_at TIMESTAMP, updated_at TIMESTAMP,
            closed_at TIMESTAMP, merged_at TIMESTAMP,
            days_to_merge INTEGER
        )
    """)

    con.execute("""
        CREATE TABLE stg_issues (
            issue_id VARCHAR, issue_number INTEGER, repo_full_name VARCHAR, title VARCHAR,
            state VARCHAR, author_login VARCHAR, comments INTEGER,
            created_at TIMESTAMP, updated_at TIMESTAMP,
            closed_at TIMESTAMP, days_to_close INTEGER, is_closed BOOLEAN
        )
    """)

    con.execute("""
        INSERT INTO stg_pull_requests VALUES
        ('1', 1, 'apache/iceberg', 'fix bug', 'closed', 'alice', true,
         'main', 'fix/1',
         current_timestamp - interval '10 days',
         current_timestamp, current_timestamp, current_timestamp, 5)
    """)

    con.execute("""
        INSERT INTO stg_issues VALUES
        ('101', 42, 'apache/iceberg', 'Bug report', 'open', 'alice', 3,
         current_timestamp - interval '5 days',
         current_timestamp, null, null, false)
    """)

    sql = load_sql("marts/contributor_churn_features.sql")
    con.execute(f"CREATE OR REPLACE VIEW contributor_churn_features AS {sql}")

    return con


class TestParquetExport:
    def test_parquet_export_produces_file(self, db_with_churn_view):
        """COPY TO parquet should produce a non-empty file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "snapshot.parquet"
            db_with_churn_view.execute(
                f"COPY contributor_churn_features TO '{out}' (FORMAT PARQUET)"
            )
            assert out.exists()
            assert out.stat().st_size > 0

    def test_parquet_export_has_expected_columns(self, db_with_churn_view):
        """Exported parquet must contain all required feature columns."""
        required_columns = [
            "developer",
            "repo_full_name",
            "snapshot_date",
            "churn_risk_score",
            "days_since_last_pr",
            "days_since_last_issue",
            "days_since_last_contribution",
            "pr_count_last_90d",
            "pr_count_prior_90d",
            "pr_frequency_last_90d",
            "pr_frequency_prior_90d",
            "comment_rate_last_90d",
            "total_prs_merged",
            "recency_component",
            "frequency_component",
            "engagement_component",
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "snapshot.parquet"
            db_with_churn_view.execute(
                f"COPY contributor_churn_features TO '{out}' (FORMAT PARQUET)"
            )
            schema = pq.read_schema(out)
            col_names = schema.names
            for col in required_columns:
                assert col in col_names, f"Missing column in parquet export: '{col}'"

    def test_churn_risk_score_is_float(self, db_with_churn_view):
        """churn_risk_score must export as a float type, not string or integer.

        DuckDB can coerce numeric columns to unexpected types depending on
        how the expression is written — this test guards against that.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "snapshot.parquet"
            db_with_churn_view.execute(
                f"COPY contributor_churn_features TO '{out}' (FORMAT PARQUET)"
            )
            schema = pq.read_schema(out)
            score_field = schema.field("churn_risk_score")
            assert pa.types.is_floating(score_field.type), (
                f"churn_risk_score should be float, got {score_field.type}"
            )

    def test_snapshot_date_is_date_type(self, db_with_churn_view):
        """snapshot_date should export as a date type, not timestamp or string."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "snapshot.parquet"
            db_with_churn_view.execute(
                f"COPY contributor_churn_features TO '{out}' (FORMAT PARQUET)"
            )
            schema = pq.read_schema(out)
            date_field = schema.field("snapshot_date")
            assert pa.types.is_date(date_field.type), (
                f"snapshot_date should be date type, got {date_field.type}"
            )

    def test_parquet_row_count_matches_duckdb(self, db_with_churn_view):
        """Row count in parquet should match what DuckDB reports for the view."""
        expected = db_with_churn_view.execute(
            "SELECT COUNT(*) FROM contributor_churn_features"
        ).fetchone()[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "snapshot.parquet"
            db_with_churn_view.execute(
                f"COPY contributor_churn_features TO '{out}' (FORMAT PARQUET)"
            )
            table = pq.read_table(out)
            assert table.num_rows == expected, (
                f"Expected {expected} rows in parquet, got {table.num_rows}"
            )
