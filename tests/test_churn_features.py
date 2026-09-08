"""
Tests for contributor_churn_features dbt model SQL logic.

Follows the same pattern as test_dbt_models.py: load SQL, strip Jinja refs,
run against an in-memory DuckDB with staged fixture tables.

Run with: pytest tests/test_churn_features.py -v
"""

import re
from pathlib import Path

import duckdb
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
def db():
    """
    In-memory DuckDB with stg_pull_requests and stg_issues pre-loaded.

    Fixtures:
      - alice: 5 recent PRs, active issue commenter → low churn risk
      - bob:   last PR 180 days ago, no recent activity → high churn risk
      - carol: 4 PRs in prior-90d window, 0 in last 90d → frequency decline
      - dave:  issues only, never filed a PR → days_since_last_pr should be NULL
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

    # alice: 5 PRs within the last 30 days — active contributor, low churn risk
    for i in range(5):
        con.execute(f"""
            INSERT INTO stg_pull_requests VALUES
            ('{200 + i}', {200 + i}, 'apache/iceberg', 'Alice PR {i}', 'closed',
             'alice', true, 'main', 'feature/{i}',
             current_timestamp - interval '{10 + i} days',
             current_timestamp, current_timestamp, current_timestamp, 3)
        """)

    # alice: issues with high comment count (engagement signal)
    for i in range(3):
        con.execute(f"""
            INSERT INTO stg_issues VALUES
            ('{100 + i}', {100 + i}, 'apache/iceberg', 'Alice issue {i}', 'open',
             'alice', 8, current_timestamp - interval '{5 + i} days',
             current_timestamp, null, null, false)
        """)

    # bob: last PR was 180 days ago — inactive, high churn risk
    con.execute("""
        INSERT INTO stg_pull_requests VALUES
        ('300', 300, 'apache/iceberg', 'Bob old PR', 'closed',
         'bob', true, 'main', 'old-feature',
         current_timestamp - interval '180 days',
         current_timestamp - interval '175 days',
         current_timestamp - interval '175 days',
         current_timestamp - interval '175 days', 5)
    """)

    # carol: 4 PRs in the prior-90d window (91–180 days ago), 0 in last 90d — frequency decline
    for i in range(4):
        con.execute(f"""
            INSERT INTO stg_pull_requests VALUES
            ('{400 + i}', {400 + i}, 'apache/iceberg', 'Carol prior PR {i}', 'closed',
             'carol', true, 'main', 'prior/{i}',
             current_timestamp - interval '{100 + i * 5} days',
             current_timestamp - interval '{95 + i * 5} days',
             current_timestamp - interval '{95 + i * 5} days',
             current_timestamp - interval '{95 + i * 5} days', 4)
        """)

    # dave: issues only, no PRs at all
    con.execute("""
        INSERT INTO stg_issues VALUES
        ('500', 500, 'apache/iceberg', 'Dave issue', 'open',
         'dave', 2, current_timestamp - interval '15 days',
         current_timestamp, null, null, false)
    """)

    # Create the churn features view from the dbt model SQL
    sql = load_sql("marts/contributor_churn_features.sql")
    con.execute(f"CREATE OR REPLACE VIEW contributor_churn_features AS {sql}")

    return con


class TestContributorChurnFeatures:
    def test_churn_risk_score_in_valid_range(self, db):
        """All churn_risk_score values must be in [0.0, 1.0]."""
        rows = db.execute("""
            SELECT developer, churn_risk_score FROM contributor_churn_features
            WHERE churn_risk_score < 0.0 OR churn_risk_score > 1.0
        """).fetchall()
        assert rows == [], f"Scores out of range: {rows}"

    def test_snapshot_date_equals_current_date(self, db):
        """snapshot_date must equal current_date for every row."""
        rows = db.execute("""
            SELECT developer FROM contributor_churn_features
            WHERE snapshot_date != current_date
        """).fetchall()
        assert rows == [], f"Rows with wrong snapshot_date: {rows}"

    def test_days_since_last_pr_null_for_issue_only_contributor(self, db):
        """dave has no PRs — days_since_last_pr should be NULL, days_since_last_issue non-null."""
        row = db.execute("""
            SELECT days_since_last_pr, days_since_last_issue
            FROM contributor_churn_features
            WHERE developer = 'dave'
        """).fetchone()
        assert row is not None, "dave not found in contributor_churn_features"
        assert row[0] is None, f"Expected NULL days_since_last_pr for dave, got {row[0]}"
        assert row[1] is not None, "Expected non-null days_since_last_issue for dave"

    def test_active_contributor_has_low_churn_score(self, db):
        """alice has 5 recent PRs and high comment rate — churn risk should be below 0.3."""
        score = db.execute("""
            SELECT churn_risk_score FROM contributor_churn_features
            WHERE developer = 'alice'
        """).fetchone()[0]
        assert score is not None
        assert score < 0.3, f"Expected alice churn score < 0.3, got {score}"

    def test_inactive_contributor_has_high_churn_score(self, db):
        """bob's last PR was 180 days ago — churn risk should be above 0.5."""
        score = db.execute("""
            SELECT churn_risk_score FROM contributor_churn_features
            WHERE developer = 'bob'
        """).fetchone()[0]
        assert score is not None
        assert score > 0.5, f"Expected bob churn score > 0.5, got {score}"

    def test_frequency_decline_raises_score(self, db):
        """carol (4 prior PRs, 0 recent) should have higher churn score than alice (5 recent PRs)."""
        scores = db.execute("""
            SELECT developer, churn_risk_score FROM contributor_churn_features
            WHERE developer IN ('alice', 'carol')
            ORDER BY developer
        """).fetchall()
        score_map = {r[0]: r[1] for r in scores}
        assert "alice" in score_map and "carol" in score_map
        assert score_map["carol"] > score_map["alice"], (
            f"carol ({score_map['carol']:.3f}) should have higher risk than alice ({score_map['alice']:.3f})"
        )

    def test_pr_count_windows_correct(self, db):
        """carol: pr_count_last_90d=0, pr_count_prior_90d=4."""
        row = db.execute("""
            SELECT pr_count_last_90d, pr_count_prior_90d
            FROM contributor_churn_features
            WHERE developer = 'carol'
        """).fetchone()
        assert row is not None, "carol not found"
        assert row[0] == 0, f"Expected pr_count_last_90d=0 for carol, got {row[0]}"
        assert row[1] == 4, f"Expected pr_count_prior_90d=4 for carol, got {row[1]}"

    def test_all_developers_present(self, db):
        """All four fixture developers should appear in the output."""
        developers = {
            r[0] for r in db.execute(
                "SELECT developer FROM contributor_churn_features"
            ).fetchall()
        }
        for name in ("alice", "bob", "carol", "dave"):
            assert name in developers, f"{name} missing from contributor_churn_features"
