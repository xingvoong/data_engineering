"""
Dagster software-defined assets for the GitHub batch ingestion pipeline.
Each asset maps to a dlt resource and tracks what was loaded.
"""

import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, asset

INGESTION_DIR = Path(__file__).parents[3] / "ingestion" / "batch"


def _run_pipeline(context: AssetExecutionContext, resource_filter: str = None) -> dict:
    """Run the dlt pipeline as a subprocess and return load info."""
    cmd = [sys.executable, str(INGESTION_DIR / "github_pipeline.py")]
    if resource_filter:
        cmd += ["--resource", resource_filter]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(INGESTION_DIR),
    )

    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dlt pipeline failed:\n{result.stderr}")

    context.log.info(result.stdout)
    return {"stdout": result.stdout}


@asset(
    group_name="ingestion",
    description="GitHub repositories indexed by data engineering topics, loaded via dlt.",
    compute_kind="dlt",
)
def raw_github_repos(context: AssetExecutionContext):
    info = _run_pipeline(context, resource_filter="repositories")
    context.add_output_metadata(
        {"pipeline_output": MetadataValue.text(info["stdout"][:2000])}
    )


@asset(
    group_name="ingestion",
    description="GitHub issues from tracked repos, loaded incrementally via dlt.",
    compute_kind="dlt",
)
def raw_github_issues(context: AssetExecutionContext):
    info = _run_pipeline(context, resource_filter="issues")
    context.add_output_metadata(
        {"pipeline_output": MetadataValue.text(info["stdout"][:2000])}
    )


@asset(
    group_name="ingestion",
    description="GitHub pull requests from tracked repos, loaded incrementally via dlt.",
    compute_kind="dlt",
)
def raw_github_pull_requests(context: AssetExecutionContext):
    info = _run_pipeline(context, resource_filter="pull_requests")
    context.add_output_metadata(
        {"pipeline_output": MetadataValue.text(info["stdout"][:2000])}
    )
