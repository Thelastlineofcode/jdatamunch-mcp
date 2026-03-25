"""Tests for index_local tool."""

import csv
import os
from pathlib import Path

import pytest

from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.storage.data_store import DataStore


def test_basic_indexing(sample_csv, storage_dir):
    result = index_local(path=sample_csv, name="sample", storage_path=storage_dir)
    assert "error" not in result
    r = result["result"]
    assert r["dataset"] == "sample"
    assert r["rows"] == 10
    assert r["columns"] == 5
    assert r.get("skipped") is not True


def test_creates_sqlite(sample_csv, storage_dir):
    index_local(path=sample_csv, name="sample", storage_path=storage_dir)
    store = DataStore(base_path=storage_dir)
    assert store.sqlite_path("sample").exists()


def test_incremental_skip(sample_csv, storage_dir):
    index_local(path=sample_csv, name="sample", storage_path=storage_dir)
    result2 = index_local(
        path=sample_csv, name="sample", incremental=True, storage_path=storage_dir
    )
    assert result2["result"].get("skipped") is True


def test_incremental_false_reindexes(sample_csv, storage_dir):
    index_local(path=sample_csv, name="sample", storage_path=storage_dir)
    result2 = index_local(
        path=sample_csv, name="sample", incremental=False, storage_path=storage_dir
    )
    assert result2["result"].get("skipped") is not True
    assert result2["result"]["rows"] == 10


def test_unknown_file_raises(storage_dir):
    result = index_local(path="/nonexistent/file.csv", storage_path=storage_dir)
    assert "error" in result or result.get("result", {}).get("rows") == 0


def test_unsupported_format(tmp_path, storage_dir):
    p = tmp_path / "data.parquet"
    p.write_bytes(b"fake")
    result = index_local(path=str(p), storage_path=storage_dir)
    assert "error" in result


def test_column_type_detection(sample_csv, storage_dir):
    index_local(path=sample_csv, name="sample", storage_path=storage_dir)
    store = DataStore(base_path=storage_dir)
    idx = store.load("sample")
    col_types = {c["name"]: c["type"] for c in idx.columns}
    assert col_types["id"] == "integer"
    assert col_types["name"] == "string"
    assert col_types["age"] in ("integer", "float")  # some rows have null → might be float
    assert col_types["score"] == "float"


def test_null_handling(sample_csv_with_nulls, storage_dir):
    result = index_local(
        path=sample_csv_with_nulls, name="nulls", storage_path=storage_dir
    )
    store = DataStore(base_path=storage_dir)
    idx = store.load("nulls")
    value_col = next(c for c in idx.columns if c["name"] == "value")
    assert value_col["null_count"] > 0


def test_meta_timing(sample_csv, storage_dir):
    result = index_local(path=sample_csv, name="sample", storage_path=storage_dir)
    assert "_meta" in result
    assert result["_meta"]["timing_ms"] >= 0
