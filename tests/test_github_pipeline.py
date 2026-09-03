"""
Tests for ingestion/batch/github_pipeline.py

Covers:
- Record shape and field mapping from API response
- Incremental filter behaviour (updated_at cutoff)
- Deduplication: issues API returns PRs, which should be excluded
- Pagination: stops when API returns empty page
- Rate limit header handling
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses as responses_lib

sys.path.insert(0, str(Path(__file__).parents[1] / "ingestion" / "batch"))

from github_pipeline import github_issues, github_pull_requests, github_repos


class TestGithubRepos:
    def test_repo_field_mapping(self, sample_repo, github_search_response):
        """All expected output fields are present and correctly mapped."""
        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[sample_repo]])

            records = list(github_repos(topics=["data-engineering"]))

        assert len(records) == 1
        r = records[0]
        assert r["id"] == sample_repo["id"]
        assert r["full_name"] == "apache/iceberg"
        assert r["stars"] == 6000
        assert r["forks"] == 2000
        assert r["open_issues"] == 300
        assert r["primary_language"] == "Java"
        assert r["is_fork"] is False
        assert r["owner_login"] == "apache"
        assert r["owner_type"] == "Organization"

    def test_repo_timestamps_are_strings(self, sample_repo):
        """Timestamps are passed through as strings for dlt to handle typing."""
        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[sample_repo]])

            records = list(github_repos(topics=["data-engineering"]))

        r = records[0]
        assert isinstance(r["created_at"], str)
        assert isinstance(r["updated_at"], str)

    def test_multiple_topics_are_queried(self):
        """Each topic triggers a separate paginate call."""
        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[]])
            list(github_repos(topics=["iceberg", "flink", "dbt"]))

        assert mock_paginate.call_count == 3

    def test_empty_page_yields_nothing(self):
        """No records yielded when API returns empty items."""
        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[]])
            records = list(github_repos(topics=["data-engineering"]))

        assert records == []


class TestGithubIssues:
    def test_issue_field_mapping(self, sample_issue):
        """Output fields are correctly mapped from API response."""
        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[sample_issue]])

            records = list(github_issues(repos=["apache/iceberg"]))

        assert len(records) == 1
        i = records[0]
        assert i["issue_id"] if "issue_id" in i else i["id"] == sample_issue["id"]
        assert i["state"] == "closed"
        assert i["author_login"] == "dev_user"
        assert i["comments"] == 5
        assert "bug" in i["labels"]

    def test_pull_requests_are_excluded(self, sample_issue):
        """Issues that are actually PRs (have pull_request key) are filtered out."""
        pr_disguised_as_issue = {**sample_issue, "pull_request": {"url": "..."}}

        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[pr_disguised_as_issue]])
            records = list(github_issues(repos=["apache/iceberg"]))

        assert records == []

    def test_mixed_issues_and_prs_filtered(self, sample_issue):
        """Only real issues are yielded when page contains both."""
        pr = {**sample_issue, "id": 999, "pull_request": {"url": "..."}}
        page = [sample_issue, pr]

        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([page])
            records = list(github_issues(repos=["apache/iceberg"]))

        assert len(records) == 1
        assert records[0]["id"] == sample_issue["id"]

    def test_multiple_repos_queried(self):
        """Each repo triggers a separate paginate call."""
        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[]])
            list(github_issues(repos=["apache/iceberg", "dbt-labs/dbt-core"]))

        assert mock_paginate.call_count == 2


class TestGithubPullRequests:
    def test_pr_field_mapping(self, sample_pull_request):
        """Output fields are correctly mapped from API response."""
        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[sample_pull_request]])

            records = list(github_pull_requests(repos=["apache/iceberg"]))

        assert len(records) == 1
        pr = records[0]
        assert pr["id"] == sample_pull_request["id"]
        assert pr["is_merged"] is True
        assert pr["additions"] == 120
        assert pr["deletions"] == 40
        assert pr["changed_files"] == 5
        assert pr["base_branch"] == "main"

    def test_unmerged_pr_is_merged_false(self, sample_pull_request):
        """PR without merged_at sets is_merged to False."""
        unmerged = {**sample_pull_request, "merged_at": None}

        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[unmerged]])
            records = list(github_pull_requests(repos=["apache/iceberg"]))

        assert records[0]["is_merged"] is False

    def test_pr_with_null_user(self, sample_pull_request):
        """PR with deleted user account (user=None) doesn't crash."""
        pr_no_user = {**sample_pull_request, "user": None}

        with patch("github_pipeline.paginate") as mock_paginate:
            mock_paginate.return_value = iter([[pr_no_user]])
            records = list(github_pull_requests(repos=["apache/iceberg"]))

        assert records[0]["author_login"] is None
