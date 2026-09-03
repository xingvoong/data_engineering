from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets.batch_ingestion import (
    raw_github_issues,
    raw_github_pull_requests,
    raw_github_repos,
)
from .assets.dbt_assets import github_dbt_assets
from .jobs.daily_pipeline import daily_schedule, github_batch_job

DBT_PROJECT_DIR = Path(__file__).parents[2] / "processing" / "dbt"

defs = Definitions(
    assets=[
        raw_github_repos,
        raw_github_issues,
        raw_github_pull_requests,
        github_dbt_assets,
    ],
    jobs=[github_batch_job],
    schedules=[daily_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
