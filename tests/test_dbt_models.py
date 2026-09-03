"""
Tests for dbt SQL logic using DuckDB directly.
Loads raw fixture data, runs the SQL from each model, and asserts on results.

Run with: pytest tests/test_dbt_models.py -v
"""

import re
from pathlib import Path

import duckdb
import pytest

DBT_DIR = Path(__file__).parents[1] / "processing" / "dbt"


def load_sql(relative_path: str) -> str:
    """Read a dbt SQL file and strip jinja refs for direct DuckDB execution."""
    path = DBT_DIR / "models" / relative_path
    sql = path.read_text()
    # Replace {{ ref('x') }} with just x
    sql = re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"\1", sql)
    # Replace {{ source('schema', 'table') }} with just table
    sql = re.sub(r"\{\{\s*source\('[^']+',\s*'(\w+)'\)\s*\}\}", r"\1", sql)
    return sql


@pytest.fixture
def db():
    """In-memory DuckDB with raw fixture tables pre-loaded."""
    con = duckdb.connect()

    con.execute("""
        CREATE TABLE repositories (
            id INTEGER, name VARCHAR, full_name VARCHAR, description VARCHAR,
            html_url VARCHAR, stars INTEGER, forks INTEGER,
            open_issues INTEGER, language VARCHAR, topics VARCHAR[],
            created_at VARCHAR, updated_at VARCHAR, pushed_at VARCHAR,
            is_fork BOOLEAN, owner_login VARCHAR, owner_type VARCHAR,
            primary_language VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE issues (
            id INTEGER, number INTEGER, repo_full_name VARCHAR, title VARCHAR,
            state VARCHAR, author_login VARCHAR, labels VARCHAR[],
            comments INTEGER, created_at VARCHAR, updated_at VARCHAR,
            closed_at VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE pull_requests (
            id INTEGER, number INTEGER, repo_full_name VARCHAR, title VARCHAR,
            state VARCHAR, author_login VARCHAR, is_merged BOOLEAN,
            base_branch VARCHAR, head_branch VARCHAR, commits INTEGER,
            additions INTEGER, deletions INTEGER, changed_files INTEGER,
            created_at VARCHAR, updated_at VARCHAR, closed_at VARCHAR,
            merged_at VARCHAR
        )
    """)

    # Insert fixtures
    con.execute("""
        INSERT INTO repositories VALUES
        (1, 'iceberg', 'apache/iceberg', 'Huge analytic tables',
         'https://github.com/apache/iceberg', 6000, 2000, 300, 'Java',
         ['data-engineering'], '2018-11-01T00:00:00Z', '2024-01-15T10:00:00Z',
         '2024-01-14T09:00:00Z', false, 'apache', 'Organization', 'Java'),
        (2, 'dbt-core', 'dbt-labs/dbt-core', 'dbt transforms data',
         'https://github.com/dbt-labs/dbt-core', 9000, 1500, 200, 'Python',
         ['data-engineering'], '2016-08-01T00:00:00Z', '2024-01-10T00:00:00Z',
         '2024-01-10T08:00:00Z', false, 'dbt-labs', 'Organization', 'Python')
    """)  # columns match dlt pipeline output: stars/forks/open_issues (not raw API names)

    con.execute("""
        INSERT INTO issues VALUES
        (101, 42, 'apache/iceberg', 'Bug in merge-on-read', 'closed',
         'alice', ['bug'], 5, '2024-01-01T00:00:00Z', '2024-01-08T00:00:00Z',
         '2024-01-08T00:00:00Z'),
        (102, 43, 'apache/iceberg', 'Feature: snapshot expiry', 'open',
         'bob', ['enhancement'], 2, '2024-01-05T00:00:00Z', '2024-01-12T00:00:00Z',
         null),
        (103, 10, 'dbt-labs/dbt-core', 'Compilation error on union', 'closed',
         'alice', ['bug'], 1, '2024-01-02T00:00:00Z', '2024-01-04T00:00:00Z',
         '2024-01-04T00:00:00Z')
    """)

    con.execute("""
        INSERT INTO pull_requests VALUES
        (201, 99, 'apache/iceberg', 'Add checkpoint support', 'closed',
         'alice', true, 'main', 'feature/checkpoint', 3, 120, 40, 5,
         '2024-01-08T00:00:00Z', '2024-01-12T00:00:00Z',
         '2024-01-12T00:00:00Z', '2024-01-12T00:00:00Z'),
        (202, 100, 'apache/iceberg', 'Fix partition spec bug', 'closed',
         'bob', false, 'main', 'fix/partition', 1, 10, 5, 2,
         '2024-01-09T00:00:00Z', '2024-01-11T00:00:00Z',
         '2024-01-11T00:00:00Z', null),
        (203, 50, 'dbt-labs/dbt-core', 'Improve compile speed', 'closed',
         'carol', true, 'main', 'perf/compile', 5, 300, 80, 12,
         '2024-01-06T00:00:00Z', '2024-01-09T00:00:00Z',
         '2024-01-09T00:00:00Z', '2024-01-09T00:00:00Z')
    """)

    return con


class TestStagingRepos:
    def test_row_count(self, db):
        sql = load_sql("staging/stg_repos.sql")
        result = db.execute(f"WITH stg_repos AS ({sql}) SELECT COUNT(*) FROM stg_repos").fetchone()
        assert result[0] == 2

    def test_repo_id_is_varchar(self, db):
        sql = load_sql("staging/stg_repos.sql")
        result = db.execute(f"WITH stg_repos AS ({sql}) SELECT repo_id FROM stg_repos WHERE repo_id = '1'").fetchone()
        assert result is not None

    def test_required_columns_present(self, db):
        sql = load_sql("staging/stg_repos.sql")
        df = db.execute(f"WITH stg_repos AS ({sql}) SELECT * FROM stg_repos LIMIT 1").df()
        for col in ["repo_id", "repo_full_name", "stars", "forks", "open_issues", "primary_language"]:
            assert col in df.columns


class TestStagingIssues:
    def test_days_to_close_computed_for_closed_issues(self, db):
        sql = load_sql("staging/stg_issues.sql")
        result = db.execute(f"""
            WITH stg_issues AS ({sql})
            SELECT days_to_close FROM stg_issues WHERE issue_id = '101'
        """).fetchone()
        assert result[0] is not None
        assert result[0] >= 0

    def test_days_to_close_null_for_open_issues(self, db):
        sql = load_sql("staging/stg_issues.sql")
        result = db.execute(f"""
            WITH stg_issues AS ({sql})
            SELECT days_to_close FROM stg_issues WHERE issue_id = '102'
        """).fetchone()
        assert result[0] is None

    def test_is_closed_derived_correctly(self, db):
        sql = load_sql("staging/stg_issues.sql")
        rows = db.execute(f"""
            WITH stg_issues AS ({sql})
            SELECT issue_id, is_closed FROM stg_issues ORDER BY issue_id
        """).fetchall()
        closed = {r[0]: r[1] for r in rows}
        assert closed["101"] is True
        assert closed["102"] is False


class TestStagingPullRequests:
    def test_total_changes_computed(self, db):
        sql = load_sql("staging/stg_pull_requests.sql")
        result = db.execute(f"""
            WITH stg_pull_requests AS ({sql})
            SELECT total_changes FROM stg_pull_requests WHERE pr_id = '201'
        """).fetchone()
        assert result[0] == 160  # 120 additions + 40 deletions

    def test_days_to_merge_null_for_unmerged(self, db):
        sql = load_sql("staging/stg_pull_requests.sql")
        result = db.execute(f"""
            WITH stg_pull_requests AS ({sql})
            SELECT days_to_merge FROM stg_pull_requests WHERE pr_id = '202'
        """).fetchone()
        assert result[0] is None

    def test_is_merged_flag(self, db):
        sql = load_sql("staging/stg_pull_requests.sql")
        rows = db.execute(f"""
            WITH stg_pull_requests AS ({sql})
            SELECT pr_id, is_merged FROM stg_pull_requests ORDER BY pr_id
        """).fetchall()
        merged = {r[0]: r[1] for r in rows}
        assert merged["201"] is True
        assert merged["202"] is False
