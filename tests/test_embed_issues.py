"""
Tests for features/vectors/embed_issues.py pure Python logic.

Does NOT test Qdrant or sentence-transformers — those are mocked.
Tests the business logic: stable ID generation, batching, collection management.

Run with: pytest tests/test_embed_issues.py -v
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

# Add project root so `features` package is importable
sys.path.insert(0, str(Path(__file__).parents[1]))

from features.vectors.embed_issues import (
    COLLECTION_NAME,
    VECTOR_DIM,
    _batched,
    _ensure_collection,
    _stable_id,
)


class TestStableId:
    def test_is_deterministic(self):
        """Calling _stable_id with the same input twice returns the same value."""
        assert _stable_id("101") == _stable_id("101")

    def test_is_in_uint63_range(self):
        """Result must fit in uint63 (Qdrant integer ID constraint)."""
        for issue_id in ("1", "999999", "apache/iceberg#42"):
            result = _stable_id(issue_id)
            assert 0 <= result < 2**63, f"ID out of range for input '{issue_id}': {result}"

    def test_different_inputs_produce_different_ids(self):
        """Different issue IDs must not collide (for our small fixture set)."""
        ids = [_stable_id(str(i)) for i in range(100)]
        assert len(set(ids)) == 100, "Hash collision detected in first 100 IDs"


class TestBatching:
    def _make_df(self, n: int) -> pd.DataFrame:
        return pd.DataFrame({"x": range(n)})

    def test_batches_correct_size(self):
        """300 rows with batch_size=256 should yield 2 batches."""
        batches = list(_batched(self._make_df(300), 256))
        assert len(batches) == 2

    def test_last_batch_smaller(self):
        """Second batch of 300 rows at batch_size=256 should have 44 rows."""
        batches = list(_batched(self._make_df(300), 256))
        assert len(batches[0]) == 256
        assert len(batches[1]) == 44

    def test_exact_multiple_produces_no_remainder(self):
        """512 rows at batch_size=256 should yield exactly 2 full batches."""
        batches = list(_batched(self._make_df(512), 256))
        assert len(batches) == 2
        assert all(len(b) == 256 for b in batches)

    def test_empty_df_yields_nothing(self):
        batches = list(_batched(self._make_df(0), 256))
        assert batches == []


class TestEnsureCollection:
    def test_creates_collection_when_missing(self):
        """When get_collection raises, create_collection should be called once."""
        mock_client = MagicMock()
        mock_client.get_collection.side_effect = Exception("Collection not found")

        _ensure_collection(mock_client)

        mock_client.create_collection.assert_called_once()
        call_kwargs = mock_client.create_collection.call_args
        assert call_kwargs.kwargs["collection_name"] == COLLECTION_NAME

    def test_does_not_recreate_existing_collection(self):
        """When get_collection succeeds, create_collection must NOT be called."""
        mock_client = MagicMock()
        mock_client.get_collection.return_value = MagicMock()

        _ensure_collection(mock_client)

        mock_client.create_collection.assert_not_called()

    def test_collection_config_uses_correct_dimensions(self):
        """The new collection must be created with VECTOR_DIM dimensions."""
        from qdrant_client.models import VectorParams

        mock_client = MagicMock()
        mock_client.get_collection.side_effect = Exception("not found")

        _ensure_collection(mock_client)

        call_kwargs = mock_client.create_collection.call_args.kwargs
        vectors_config = call_kwargs["vectors_config"]
        assert isinstance(vectors_config, VectorParams)
        assert vectors_config.size == VECTOR_DIM


class TestBatchLoopLogic:
    def test_batching_splits_300_rows_into_two_batches(self):
        """Directly verify _batched produces 2 batches for 300 rows at size 256."""
        df = pd.DataFrame({
            "issue_id": [str(i) for i in range(300)],
            "repo_full_name": ["apache/iceberg"] * 300,
            "title": ["test issue"] * 300,
            "state": ["open"] * 300,
            "created_at": ["2024-01-01"] * 300,
        })
        batches = list(_batched(df, 256))
        assert len(batches) == 2
        assert len(batches[0]) == 256
        assert len(batches[1]) == 44
