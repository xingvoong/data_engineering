"""
Shared fixtures for Phase 1 tests.
"""

import pytest


@pytest.fixture
def sample_repo():
    return {
        "id": 1,
        "name": "iceberg",
        "full_name": "apache/iceberg",
        "description": "Apache Iceberg is a high-performance format for huge analytic tables.",
        "html_url": "https://github.com/apache/iceberg",
        "stargazers_count": 6000,
        "forks_count": 2000,
        "open_issues_count": 300,
        "language": "Java",
        "topics": ["data-engineering", "apache-iceberg"],
        "created_at": "2018-11-01T00:00:00Z",
        "updated_at": "2024-01-15T10:00:00Z",
        "pushed_at": "2024-01-15T09:00:00Z",
        "fork": False,
        "owner": {"login": "apache", "type": "Organization"},
    }


@pytest.fixture
def sample_issue():
    return {
        "id": 101,
        "number": 42,
        "title": "Support partition evolution in Flink sink",
        "state": "closed",
        "user": {"login": "dev_user"},
        "labels": [{"name": "bug"}, {"name": "flink"}],
        "comments": 5,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-10T00:00:00Z",
        "closed_at": "2024-01-10T00:00:00Z",
    }


@pytest.fixture
def sample_pull_request():
    return {
        "id": 201,
        "number": 99,
        "title": "Add incremental checkpoint support",
        "state": "closed",
        "user": {"login": "contributor"},
        "merged_at": "2024-01-12T00:00:00Z",
        "base": {"ref": "main"},
        "head": {"ref": "feature/checkpoints"},
        "commits": 3,
        "additions": 120,
        "deletions": 40,
        "changed_files": 5,
        "created_at": "2024-01-08T00:00:00Z",
        "updated_at": "2024-01-12T00:00:00Z",
        "closed_at": "2024-01-12T00:00:00Z",
    }


@pytest.fixture
def github_search_response(sample_repo):
    """Simulates the GitHub search/repositories API response."""
    return {"items": [sample_repo], "total_count": 1}


@pytest.fixture
def github_issues_response(sample_issue):
    return [sample_issue]


@pytest.fixture
def github_prs_response(sample_pull_request):
    return [sample_pull_request]
