"""Tests for aggregate tool."""

import pytest

from jdatamunch_mcp.tools.aggregate import aggregate


def test_count_all(indexed_sample):
    result = aggregate(
        dataset="sample",
        aggregations=[{"column": "*", "function": "count", "alias": "total"}],
        storage_path=indexed_sample,
    )
    assert "error" not in result
    groups = result["result"]["groups"]
    assert len(groups) == 1
    assert groups[0]["total"] == 10


def test_group_by_count(indexed_sample):
    result = aggregate(
        dataset="sample",
        aggregations=[{"column": "*", "function": "count", "alias": "cnt"}],
        group_by=["city"],
        storage_path=indexed_sample,
    )
    assert "error" not in result
    r = result["result"]
    # Sum of counts should equal total rows
    total = sum(g["cnt"] for g in r["groups"])
    assert total == 10


def test_aggregate_avg(indexed_sample):
    result = aggregate(
        dataset="sample",
        aggregations=[{"column": "score", "function": "avg", "alias": "avg_score"}],
        storage_path=indexed_sample,
    )
    groups = result["result"]["groups"]
    assert groups[0]["avg_score"] is not None


def test_aggregate_with_filter(indexed_sample):
    result = aggregate(
        dataset="sample",
        aggregations=[{"column": "*", "function": "count", "alias": "cnt"}],
        filters=[{"column": "city", "op": "eq", "value": "Hollywood"}],
        storage_path=indexed_sample,
    )
    r = result["result"]
    assert r["groups"][0]["cnt"] == 4  # Alice, Charlie, Eve, Iris are in Hollywood


def test_aggregate_no_aggregations(indexed_sample):
    result = aggregate(
        dataset="sample",
        aggregations=[],
        storage_path=indexed_sample,
    )
    assert "error" in result


def test_aggregate_invalid_column(indexed_sample):
    result = aggregate(
        dataset="sample",
        aggregations=[{"column": "bogus", "function": "sum", "alias": "s"}],
        storage_path=indexed_sample,
    )
    assert "error" in result


def test_aggregate_not_indexed(storage_dir):
    result = aggregate(
        dataset="ghost",
        aggregations=[{"column": "*", "function": "count"}],
        storage_path=storage_dir,
    )
    assert "error" in result
    assert "NOT_INDEXED" in result["error"]
