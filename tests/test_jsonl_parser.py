"""Tests for JSONL parser and index_local integration."""

import json
import pytest

from jdatamunch_mcp.parser.jsonl_parser import parse_jsonl
from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.storage.data_store import DataStore


def test_parse_jsonl_columns(sample_jsonl):
    parsed = parse_jsonl(sample_jsonl)
    assert len(parsed.columns) == 5
    assert [c.name for c in parsed.columns] == ["id", "name", "age", "city", "score"]


def test_parse_jsonl_rows(sample_jsonl):
    parsed = parse_jsonl(sample_jsonl)
    rows = list(parsed.row_iterator)
    assert len(rows) == 10
    assert rows[0][1] == "Alice"
    assert rows[0][0] == "1"


def test_parse_jsonl_null_handling(sample_jsonl):
    parsed = parse_jsonl(sample_jsonl)
    rows = list(parsed.row_iterator)
    frank = rows[5]
    assert frank[2] == ""   # age is None
    assert frank[4] == ""   # score is None


def test_parse_jsonl_sparse_keys(tmp_path):
    """Objects with missing keys should yield empty string for those columns."""
    path = tmp_path / "sparse.jsonl"
    with open(path, "w") as f:
        f.write(json.dumps({"id": 1, "name": "Alice", "extra": "yes"}) + "\n")
        f.write(json.dumps({"id": 2, "name": "Bob"}) + "\n")  # missing "extra"
    parsed = parse_jsonl(str(path))
    rows = list(parsed.row_iterator)
    assert rows[1][2] == ""  # extra is missing → ""


def test_parse_jsonl_skip_bad_lines(tmp_path):
    """Malformed JSON lines are silently skipped."""
    path = tmp_path / "bad.jsonl"
    with open(path, "w") as f:
        f.write(json.dumps({"id": 1, "val": "a"}) + "\n")
        f.write("not valid json\n")
        f.write(json.dumps({"id": 2, "val": "b"}) + "\n")
    parsed = parse_jsonl(str(path))
    rows = list(parsed.row_iterator)
    assert len(rows) == 2


def test_parse_jsonl_empty_file(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_text("")
    with pytest.raises(ValueError, match="No valid JSON objects"):
        parse_jsonl(str(path))


def test_parse_jsonl_metadata(sample_jsonl):
    parsed = parse_jsonl(sample_jsonl)
    assert parsed.metadata["estimated_rows"] > 0
    assert parsed.metadata["file_size"] > 0


def test_index_jsonl(sample_jsonl, storage_dir):
    result = index_local(path=sample_jsonl, name="sample-jsonl", storage_path=storage_dir)
    assert "error" not in result
    r = result["result"]
    assert r["rows"] == 10
    assert r["columns"] == 5
    assert r.get("skipped") is not True


def test_index_jsonl_column_types(sample_jsonl, storage_dir):
    index_local(path=sample_jsonl, name="sample-jsonl", storage_path=storage_dir)
    store = DataStore(base_path=storage_dir)
    idx = store.load("sample-jsonl")
    col_types = {c["name"]: c["type"] for c in idx.columns}
    assert col_types["id"] == "integer"
    assert col_types["name"] == "string"
    assert col_types["score"] == "float"


def test_jsonl_nested_objects(tmp_path, storage_dir):
    """Nested dicts become string type."""
    path = tmp_path / "nested.jsonl"
    with open(path, "w") as f:
        for i in range(5):
            f.write(json.dumps({"id": i, "meta": {"key": "val"}}) + "\n")
    result = index_local(path=str(path), name="nested", storage_path=storage_dir)
    assert "error" not in result
    store = DataStore(base_path=storage_dir)
    idx = store.load("nested")
    col_types = {c["name"]: c["type"] for c in idx.columns}
    assert col_types["meta"] == "string"


def test_jsonl_ndjson_extension(tmp_path, storage_dir):
    """.ndjson extension is routed correctly."""
    path = tmp_path / "data.ndjson"
    with open(path, "w") as f:
        for i in range(3):
            f.write(json.dumps({"id": i, "val": str(i)}) + "\n")
    result = index_local(path=str(path), name="ndjson-test", storage_path=storage_dir)
    assert "error" not in result
    assert result["result"]["rows"] == 3
