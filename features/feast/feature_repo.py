"""
Feast feature repository for GitHub Pulse contributor churn prediction.

Defines the Entity, FeatureView, and FeatureService that describe contributor
activity features. The offline store reads parquet files exported from DuckDB
(via materialize.py). The online store is SQLite for local dev.

Usage:
    feast -c features/feast/ apply
    python features/feast/materialize.py --start 2026-09-07 --end 2026-09-08
    feast -c features/feast/ materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)
"""

from datetime import timedelta

from pathlib import Path

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float32, Float64, Int64

# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------

contributor = Entity(
    name="contributor",
    description="GitHub login of the contributor (author_login).",
    join_keys=["developer"],
)

# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------
# Points at the parquet directory exported by materialize.py.
# Path is relative to the feature_store.yaml location (features/feast/).

_PARQUET_PATH = str(
    Path(__file__).parent / "data" / "parquet" / "contributor_churn_features"
)

contributor_churn_source = FileSource(
    path=_PARQUET_PATH,
    timestamp_field="snapshot_date",
)

# ---------------------------------------------------------------------------
# Feature view
# ---------------------------------------------------------------------------

contributor_activity_fv = FeatureView(
    name="contributor_activity",
    entities=[contributor],
    ttl=timedelta(days=7),
    schema=[
        # Recency (nullable in DuckDB — use Float32 to handle NULLs as NaN)
        Field(name="days_since_last_pr", dtype=Float32),
        Field(name="days_since_last_issue", dtype=Float32),
        Field(name="days_since_last_contribution", dtype=Float32),
        # Frequency windows
        Field(name="pr_count_last_90d", dtype=Int64),
        Field(name="pr_count_prior_90d", dtype=Int64),
        Field(name="pr_frequency_last_90d", dtype=Float64),
        Field(name="pr_frequency_prior_90d", dtype=Float64),
        # Engagement
        Field(name="comment_rate_last_90d", dtype=Float64),
        # All-time
        Field(name="total_prs_merged", dtype=Int64),
        # Score components (useful for model explainability)
        Field(name="recency_component", dtype=Float64),
        Field(name="frequency_component", dtype=Float64),
        Field(name="engagement_component", dtype=Float64),
        # Final churn risk score (0–1)
        Field(name="churn_risk_score", dtype=Float64),
    ],
    source=contributor_churn_source,
    online=True,
)

# ---------------------------------------------------------------------------
# Feature service
# ---------------------------------------------------------------------------
# Bundles the features consumed by the churn prediction model.
# Version the service name (v1) so a breaking schema change can be
# introduced as v2 without breaking existing consumers.

churn_prediction_fs = FeatureService(
    name="churn_prediction_v1",
    features=[contributor_activity_fv],
    description="Contributor activity features for churn risk scoring.",
)
