"""
Dagster assets for dbt transformations using dagster-dbt.
Runs after ingestion assets complete.
"""

from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parents[3] / "processing" / "dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    name="github_dbt_assets",
)
def github_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
