"""Shared fixtures for jdatamunch-mcp tests."""

import csv
import json
import os
import tempfile
from pathlib import Path

import pytest

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

try:
    import xlrd  # noqa: F401
    HAS_XLRD = True
except ImportError:
    HAS_XLRD = False

try:
    import pyarrow  # noqa: F401
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


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


_SAMPLE_ROWS = [
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
]
_SAMPLE_HEADER = ["id", "name", "age", "city", "score"]


@pytest.fixture
def sample_xlsx(tmp_path):
    """Small .xlsx file matching sample_csv schema."""
    pytest.importorskip("openpyxl")
    import openpyxl
    path = tmp_path / "sample.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(_SAMPLE_HEADER)
    for row in _SAMPLE_ROWS:
        ws.append(row)
    wb.save(str(path))
    return str(path)


@pytest.fixture
def sample_xls(tmp_path):
    """Small .xls file matching sample_csv schema."""
    pytest.importorskip("xlrd")
    try:
        import xlwt
    except ImportError:
        pytest.skip("xlwt required to create .xls fixtures (pip install xlwt)")
    path = tmp_path / "sample.xls"
    wb = xlwt.Workbook()
    ws = wb.add_sheet("data")
    for col, name in enumerate(_SAMPLE_HEADER):
        ws.write(0, col, name)
    for row_idx, row in enumerate(_SAMPLE_ROWS, start=1):
        for col_idx, val in enumerate(row):
            if val is not None:
                ws.write(row_idx, col_idx, val)
    wb.save(str(path))
    return str(path)


@pytest.fixture
def sample_parquet(tmp_path):
    """Small .parquet file matching sample_csv schema."""
    pytest.importorskip("pyarrow")
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = tmp_path / "sample.parquet"
    table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], type=pa.int64()),
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"],
        "age": pa.array([30, 25, 35, 28, 32, None, 27, 45, 22, 38], type=pa.int64()),
        "city": ["Hollywood", "Central", "Hollywood", "Pacific", "Hollywood", "Central", "Pacific", "N Hollywood", "Hollywood", "Central"],
        "score": pa.array([9.5, 7.2, 8.8, 6.1, 9.0, None, 8.3, 7.9, 9.2, 5.5], type=pa.float64()),
    })
    pq.write_table(table, str(path))
    return str(path)


@pytest.fixture
def sample_jsonl(tmp_path):
    """Small .jsonl file matching sample_csv schema."""
    path = tmp_path / "sample.jsonl"
    rows = [
        {"id": 1, "name": "Alice", "age": 30, "city": "Hollywood", "score": 9.5},
        {"id": 2, "name": "Bob", "age": 25, "city": "Central", "score": 7.2},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Hollywood", "score": 8.8},
        {"id": 4, "name": "Diana", "age": 28, "city": "Pacific", "score": 6.1},
        {"id": 5, "name": "Eve", "age": 32, "city": "Hollywood", "score": 9.0},
        {"id": 6, "name": "Frank", "age": None, "city": "Central", "score": None},
        {"id": 7, "name": "Grace", "age": 27, "city": "Pacific", "score": 8.3},
        {"id": 8, "name": "Henry", "age": 45, "city": "N Hollywood", "score": 7.9},
        {"id": 9, "name": "Iris", "age": 22, "city": "Hollywood", "score": 9.2},
        {"id": 10, "name": "Jack", "age": 38, "city": "Central", "score": 5.5},
    ]
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
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
