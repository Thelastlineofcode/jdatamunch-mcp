"""Tests for search_data tool."""

import pytest

from jdatamunch_mcp.tools.search_data import search_data


def test_search_by_column_name(indexed_sample):
    result = search_data(dataset="sample", query="city", storage_path=indexed_sample)
    assert "error" not in result
    names = [r["name"] for r in result["result"]]
    assert "city" in names


def test_search_by_value(indexed_sample):
    result = search_data(dataset="sample", query="Hollywood", storage_path=indexed_sample)
    assert "error" not in result
    results = result["result"]
    assert len(results) > 0
    # The city column should be in results
    names = [r["name"] for r in results]
    assert "city" in names


def test_search_returns_ids(indexed_sample):
    result = search_data(dataset="sample", query="name", storage_path=indexed_sample)
    for r in result["result"]:
        assert r["id"].endswith("#column")
        assert "::" in r["id"]


def test_search_scope_schema(indexed_sample):
    result = search_data(
        dataset="sample", query="age", search_scope="schema", storage_path=indexed_sample
    )
    # Schema-only search — should find 'age' column
    names = [r["name"] for r in result["result"]]
    assert "age" in names


def test_search_scope_values(indexed_sample):
    result = search_data(
        dataset="sample", query="Hollywood", search_scope="values", storage_path=indexed_sample
    )
    assert "error" not in result
    # Should find the city column via value index
    names = [r["name"] for r in result["result"]]
    assert "city" in names


def test_search_not_indexed(storage_dir):
    result = search_data(dataset="ghost", query="foo", storage_path=storage_dir)
    assert "error" in result


def test_search_max_results(indexed_sample):
    result = search_data(
        dataset="sample", query="a", max_results=2, storage_path=indexed_sample
    )
    assert len(result["result"]) <= 2


def test_search_score_range(indexed_sample):
    result = search_data(dataset="sample", query="name", storage_path=indexed_sample)
    for r in result["result"]:
        assert 0.0 <= r["score"] <= 1.0
