"""Shared fixtures for jdatamunch-mcp tests."""

import csv
import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_dir(tmp_path):
    """Temporary directory for test storage."""
    return tmp_path


@pytest.fixture
def storage_dir(tmp_path):
    """Dedicated storage directory."""
    d = tmp_path / "data-index"
    d.mkdir()
    return str(d)


@pytest.fixture
def sample_csv(tmp_path):
    """Small CSV file for unit tests."""
    path = tmp_path / "sample.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "city", "score"])
        writer.writerows([
            [1, "Alice", 30, "Hollywood", 9.5],
            [2, "Bob", 25, "Central", 7.2],
            [3, "Charlie", 35, "Hollywood", 8.8],
            [4, "Diana", 28, "Pacific", 6.1],
            [5, "Eve", 32, "Hollywood", 9.0],
            [6, "Frank", None, "Central", None],
            [7, "Grace", 27, "Pacific", 8.3],
            [8, "Henry", 45, "N Hollywood", 7.9],
            [9, "Iris", 22, "Hollywood", 9.2],
            [10, "Jack", 38, "Central", 5.5],
        ])
    return str(path)


@pytest.fixture
def sample_csv_with_nulls(tmp_path):
    """CSV with various null representations."""
    path = tmp_path / "nulls.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "value", "category"])
        writer.writerows([
            [1, "100", "A"],
            [2, "N/A", "B"],
            [3, "", "A"],
            [4, "NULL", "C"],
            [5, "200", "B"],
        ])
    return str(path)


@pytest.fixture
def indexed_sample(sample_csv, storage_dir):
    """Pre-indexed sample dataset."""
    from jdatamunch_mcp.tools.index_local import index_local
    result = index_local(
        path=sample_csv,
        name="sample",
        storage_path=storage_dir,
    )
    assert "error" not in result, f"Indexing failed: {result}"
    return storage_dir
