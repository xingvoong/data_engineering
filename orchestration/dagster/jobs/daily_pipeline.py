"""
Daily batch pipeline job: ingest from GitHub → transform with dbt.
Runs at 06:00 UTC every day.
"""

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
)

github_batch_job = define_asset_job(
    name="github_batch_pipeline",
    selection=AssetSelection.groups("ingestion") | AssetSelection.all(),
    description="Full batch pipeline: GitHub ingestion via dlt, then dbt transformations.",
)

daily_schedule = ScheduleDefinition(
    job=github_batch_job,
    cron_schedule="0 6 * * *",
    name="daily_github_pipeline",
    description="Runs the full batch pipeline at 06:00 UTC daily.",
)
