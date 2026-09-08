"""
embed_issues.py — Embed GitHub issues from DuckDB into Qdrant using MiniLM.

Model: sentence-transformers/all-MiniLM-L6-v2
  - 80 MB, CPU-only, no API key required
  - 384-dim vectors, cosine similarity

Idempotent: uses upsert with stable point IDs derived from issue_id.
Running this script twice does not duplicate data in Qdrant.

Usage:
    python features/vectors/embed_issues.py
    python features/vectors/embed_issues.py --duckdb-path github_pulse.duckdb --qdrant-url http://localhost:6333
"""

from __future__ import annotations

import hashlib
import math
from typing import Iterator

import duckdb
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

COLLECTION_NAME = "github_issues"
VECTOR_DIM = 384


def _stable_id(issue_id: str) -> int:
    """Convert string issue_id to deterministic uint63 for Qdrant.

    Qdrant requires integer or UUID point IDs. We derive a stable uint63
    from the issue_id so reruns produce the same ID and upsert correctly.
    """
    return int(hashlib.sha256(issue_id.encode()).hexdigest(), 16) % (2**63)


def _ensure_collection(client: QdrantClient) -> None:
    """Create collection if it doesn't exist. Does not recreate if already present.

    Using get_collection + except rather than recreate_collection because
    recreate_collection would wipe all existing vectors on every run.
    """
    try:
        client.get_collection(COLLECTION_NAME)
        print(f"Collection '{COLLECTION_NAME}' already exists — skipping creation.")
    except Exception:
        print(f"Creating collection '{COLLECTION_NAME}' ({VECTOR_DIM}-dim, cosine).")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
        )


def _batched(df: pd.DataFrame, batch_size: int) -> Iterator[pd.DataFrame]:
    """Yield successive batch_size slices of a DataFrame."""
    n = len(df)
    for start in range(0, n, batch_size):
        yield df.iloc[start : start + batch_size]


def main(duckdb_path: str, qdrant_url: str, batch_size: int = 256) -> None:
    """Load issues from DuckDB, embed with MiniLM, upsert into Qdrant."""
    # --- Load issues from stg_issues (cleaned types from dbt) ---
    con = duckdb.connect(duckdb_path, read_only=True)
    df: pd.DataFrame = con.execute(
        """
        SELECT
            issue_id,
            repo_full_name,
            title,
            state,
            CAST(created_at AS VARCHAR) AS created_at
        FROM stg_issues
        WHERE title IS NOT NULL AND title != ''
        """
    ).df()
    con.close()

    total = len(df)
    print(f"Loaded {total} issues from '{duckdb_path}'.")

    if total == 0:
        print("No issues to embed. Exiting.")
        return

    # --- Connect to Qdrant and ensure collection exists ---
    client = QdrantClient(url=qdrant_url)
    _ensure_collection(client)

    # --- Load model once before the batch loop ---
    from sentence_transformers import SentenceTransformer

    print("Loading sentence-transformers/all-MiniLM-L6-v2 ...")
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    print("Model loaded.")

    # --- Batch encode + upsert ---
    n_batches = math.ceil(total / batch_size)
    total_upserted = 0

    for batch_num, batch_df in enumerate(_batched(df, batch_size), start=1):
        row_start = (batch_num - 1) * batch_size
        row_end = row_start + len(batch_df) - 1
        print(f"Batch {batch_num}/{n_batches} — rows {row_start}–{row_end} ({len(batch_df)} issues)")

        texts = batch_df["title"].tolist()
        # sentence-transformers handles its own internal batching; show_progress_bar
        # gives a tqdm bar per batch so progress is visible for large datasets
        vectors = model.encode(texts, show_progress_bar=True, batch_size=batch_size)

        points = [
            PointStruct(
                id=_stable_id(row["issue_id"]),
                vector=vector.tolist(),
                payload={
                    "issue_id": row["issue_id"],
                    "repo_full_name": row["repo_full_name"],
                    "title": row["title"],
                    "state": row["state"],
                    "created_at": row["created_at"],
                },
            )
            for row, vector in zip(batch_df.to_dict("records"), vectors)
        ]

        client.upsert(collection_name=COLLECTION_NAME, points=points)
        total_upserted += len(points)

    # --- Summary ---
    print(f"\nDone. Total upserted: {total_upserted}")
    info = client.get_collection(COLLECTION_NAME)
    print(f"Collection '{COLLECTION_NAME}': {info.points_count} points total.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Embed GitHub issues and upsert to Qdrant.")
    parser.add_argument("--duckdb-path", default="github_pulse.duckdb")
    parser.add_argument("--qdrant-url", default="http://localhost:6333")
    parser.add_argument("--batch-size", type=int, default=256)
    args = parser.parse_args()
    main(args.duckdb_path, args.qdrant_url, args.batch_size)
