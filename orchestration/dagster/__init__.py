from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets.batch_ingestion import (
    raw_github_issues,
    raw_github_pull_requests,
    raw_github_repos,
)
from .assets.dbt_assets import github_dbt_assets
from .assets.feature_assets import (
    churn_features_parquet,
    feast_materialized_features,
    issue_embeddings,
)
from .jobs.daily_pipeline import daily_schedule, github_batch_job
from .sensors.kafka_sensor import kafka_lag_sensor

DBT_PROJECT_DIR = Path(__file__).parents[2] / "processing" / "dbt"

defs = Definitions(
    assets=[
        raw_github_repos,
        raw_github_issues,
        raw_github_pull_requests,
        github_dbt_assets,
        churn_features_parquet,
        feast_materialized_features,
        issue_embeddings,
    ],
    jobs=[github_batch_job],
    schedules=[daily_schedule],
    sensors=[kafka_lag_sensor],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
