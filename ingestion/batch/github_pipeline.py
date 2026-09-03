"""
GitHub batch ingestion pipeline using dlt.
Ingests repos, issues, pull requests, and commits incrementally.
Writes to MinIO (S3-compatible) as Parquet files for Iceberg consumption.
"""

import os
from datetime import datetime, timezone
from typing import Iterator

import dlt
import requests
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
BASE_URL = "https://api.github.com"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


def paginate(url: str, params: dict = None, max_pages: int = 10) -> Iterator[list]:
    """Yield pages of results from a GitHub API endpoint.

    GitHub Search API caps at 1000 results (10 pages × 100). Stops cleanly at max_pages.
    """
    import time

    params = params or {}
    params["per_page"] = 100
    page = 1

    while page <= max_pages:
        params["page"] = page
        response = requests.get(url, headers=HEADERS, params=params)

        # Search API returns 422 past page 10 — stop cleanly
        if response.status_code == 422:
            break

        response.raise_for_status()
        data = response.json()

        items = data if isinstance(data, list) else data.get("items", data)
        if not items:
            break

        yield items
        page += 1

        # Respect rate limit
        remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
        if remaining == 0:
            reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
            wait = max(0, reset_time - int(datetime.now(timezone.utc).timestamp()))
            print(f"Rate limit hit. Waiting {wait}s...")
            time.sleep(wait + 1)


@dlt.resource(
    name="repositories",
    write_disposition="merge",
    primary_key="id",
)
def github_repos(
    topics: list[str] = None,
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "updated_at", initial_value="2020-01-01T00:00:00Z"
    ),
) -> Iterator[dict]:
    """Fetch GitHub repositories by topic, incrementally by updated_at."""
    topics = topics or ["data-engineering", "apache-iceberg", "apache-flink"]

    for topic in topics:
        print(f"Fetching repos for topic: {topic}")
        params = {
            "q": f"topic:{topic}",
            "sort": "updated",
            "order": "desc",
        }
        for items in paginate(f"{BASE_URL}/search/repositories", params=params):
            for repo in items:
                yield {
                    "id": repo["id"],
                    "name": repo["name"],
                    "full_name": repo["full_name"],
                    "description": repo.get("description"),
                    "html_url": repo["html_url"],
                    "stars": repo["stargazers_count"],
                    "forks": repo["forks_count"],
                    "open_issues": repo["open_issues_count"],
                    "primary_language": repo.get("language"),
                    "topics": repo.get("topics", []),
                    "created_at": repo["created_at"],
                    "updated_at": repo["updated_at"],
                    "pushed_at": repo.get("pushed_at"),
                    "is_fork": repo["fork"],
                    "owner_login": repo["owner"]["login"],
                    "owner_type": repo["owner"]["type"],
                }


@dlt.resource(
    name="issues",
    write_disposition="merge",
    primary_key="id",
)
def github_issues(
    repos: list[str] = None,
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "updated_at", initial_value="2020-01-01T00:00:00Z"
    ),
) -> Iterator[dict]:
    """Fetch issues for given repos incrementally."""
    repos = repos or [
        "apache/iceberg",
        "dagster-io/dagster",
        "dbt-labs/dbt-core",
        "apache/flink",
    ]

    for repo in repos:
        print(f"Fetching issues for: {repo}")
        params = {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
            "since": updated_at.last_value,
        }
        for items in paginate(f"{BASE_URL}/repos/{repo}/issues", params=params, max_pages=50):
            for issue in items:
                # GitHub issues API returns PRs too — filter them out
                if "pull_request" in issue:
                    continue
                yield {
                    "id": issue["id"],
                    "number": issue["number"],
                    "repo_full_name": repo,
                    "title": issue["title"],
                    "state": issue["state"],
                    "author_login": issue["user"]["login"] if issue.get("user") else None,
                    "labels": [l["name"] for l in issue.get("labels", [])],
                    "comments": issue["comments"],
                    "created_at": issue["created_at"],
                    "updated_at": issue["updated_at"],
                    "closed_at": issue.get("closed_at"),
                }


@dlt.resource(
    name="pull_requests",
    write_disposition="merge",
    primary_key="id",
)
def github_pull_requests(
    repos: list[str] = None,
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "updated_at", initial_value="2020-01-01T00:00:00Z"
    ),
) -> Iterator[dict]:
    """Fetch pull requests for given repos incrementally."""
    repos = repos or [
        "apache/iceberg",
        "dagster-io/dagster",
        "dbt-labs/dbt-core",
        "apache/flink",
    ]

    for repo in repos:
        print(f"Fetching PRs for: {repo}")
        params = {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
        }
        for items in paginate(f"{BASE_URL}/repos/{repo}/pulls", params=params, max_pages=50):
            for pr in items:
                yield {
                    "id": pr["id"],
                    "number": pr["number"],
                    "repo_full_name": repo,
                    "title": pr["title"],
                    "state": pr["state"],
                    "author_login": pr["user"]["login"] if pr.get("user") else None,
                    "is_merged": pr.get("merged_at") is not None,
                    "base_branch": pr["base"]["ref"],
                    "head_branch": pr["head"]["ref"],
                    "commits": pr.get("commits"),
                    "additions": pr.get("additions"),
                    "deletions": pr.get("deletions"),
                    "changed_files": pr.get("changed_files"),
                    "created_at": pr["created_at"],
                    "updated_at": pr["updated_at"],
                    "closed_at": pr.get("closed_at"),
                    "merged_at": pr.get("merged_at"),
                }


@dlt.source(name="github")
def github_source(
    topics: list[str] = None,
    repos: list[str] = None,
):
    return [
        github_repos(topics=topics),
        github_issues(repos=repos),
        github_pull_requests(repos=repos),
    ]


if __name__ == "__main__":
    import os

    # Use local filesystem in dev (no Docker/MinIO needed).
    # Set DESTINATION=s3 in .env to write to MinIO instead.
    destination = os.getenv("DESTINATION", "duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="github_pulse",
        destination=destination,
        dataset_name="github_raw",
    )

    load_info = pipeline.run(
        github_source(
            topics=["data-engineering", "apache-iceberg", "dbt", "apache-flink"],
            repos=[
                "apache/iceberg",
                "dagster-io/dagster",
                "dbt-labs/dbt-core",
                "apache/flink",
                "apache/kafka",
            ],
        )
    )

    print(load_info)
    print(pipeline.last_trace)
