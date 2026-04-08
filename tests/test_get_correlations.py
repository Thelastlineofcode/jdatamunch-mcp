"""Tests for get_correlations tool."""

import csv
import math
import pytest

from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.tools.get_correlations import get_correlations


@pytest.fixture
def correlated_csv(tmp_path):
    """CSV with known correlations: b = 2*a (perfect), c = random-ish, d = -a."""
    path = tmp_path / "correlated.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["a", "b", "c", "d", "label"])
        for i in range(100):
            a = i
            b = 2 * i          # perfect positive correlation with a
            c = (i * 7 + 13) % 41  # pseudo-random, weak correlation
            d = -i              # perfect negative correlation with a
            writer.writerow([a, b, c, d, f"row_{i}"])
    return str(path)


@pytest.fixture
def indexed_correlated(correlated_csv, storage_dir):
    """Pre-indexed correlated dataset."""
    result = index_local(path=correlated_csv, name="corr-test", storage_path=storage_dir)
    assert "error" not in result
    return storage_dir


class TestGetCorrelations:

    def test_perfect_positive(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            min_abs_correlation=0.0,
            storage_path=indexed_correlated,
        )
        assert "error" not in result
        pairs = result["result"]["correlations"]

        # Find a-b pair
        ab = next((p for p in pairs if {p["column_a"], p["column_b"]} == {"a", "b"}), None)
        assert ab is not None
        assert ab["r"] == pytest.approx(1.0, abs=0.001)
        assert ab["direction"] == "positive"
        assert ab["strength"] == "very strong"

    def test_perfect_negative(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            min_abs_correlation=0.0,
            storage_path=indexed_correlated,
        )
        pairs = result["result"]["correlations"]

        # Find a-d pair (perfect negative)
        ad = next((p for p in pairs if {p["column_a"], p["column_b"]} == {"a", "d"}), None)
        assert ad is not None
        assert ad["r"] == pytest.approx(-1.0, abs=0.001)
        assert ad["direction"] == "negative"
        assert ad["strength"] == "very strong"

    def test_min_threshold_filters(self, indexed_correlated):
        # With high threshold, only strong correlations returned
        result = get_correlations(
            dataset="corr-test",
            min_abs_correlation=0.9,
            storage_path=indexed_correlated,
        )
        pairs = result["result"]["correlations"]
        for p in pairs:
            assert abs(p["r"]) >= 0.9

    def test_sorted_by_abs_r(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            min_abs_correlation=0.0,
            storage_path=indexed_correlated,
        )
        pairs = result["result"]["correlations"]
        abs_rs = [abs(p["r"]) for p in pairs]
        assert abs_rs == sorted(abs_rs, reverse=True)

    def test_top_n_limits(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            min_abs_correlation=0.0,
            top_n=2,
            storage_path=indexed_correlated,
        )
        assert len(result["result"]["correlations"]) <= 2

    def test_column_filter(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            columns=["a", "b"],
            min_abs_correlation=0.0,
            storage_path=indexed_correlated,
        )
        pairs = result["result"]["correlations"]
        assert len(pairs) == 1  # only one pair: a-b
        assert pairs[0]["r"] == pytest.approx(1.0, abs=0.001)

    def test_invalid_column(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            columns=["a", "nonexistent"],
            storage_path=indexed_correlated,
        )
        assert "error" in result

    def test_not_indexed(self, storage_dir):
        result = get_correlations(dataset="nope", storage_path=storage_dir)
        assert "error" in result

    def test_excludes_string_columns(self, indexed_correlated):
        """The 'label' column is string — should not appear in correlations."""
        result = get_correlations(
            dataset="corr-test",
            min_abs_correlation=0.0,
            storage_path=indexed_correlated,
        )
        pairs = result["result"]["correlations"]
        all_cols = set()
        for p in pairs:
            all_cols.add(p["column_a"])
            all_cols.add(p["column_b"])
        assert "label" not in all_cols

    def test_metadata_present(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            storage_path=indexed_correlated,
        )
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]
        r = result["result"]
        assert r["numeric_columns"] == 4
        assert r["pairs_computed"] == 6  # C(4,2)

    def test_strength_labels(self, indexed_correlated):
        result = get_correlations(
            dataset="corr-test",
            min_abs_correlation=0.0,
            storage_path=indexed_correlated,
        )
        pairs = result["result"]["correlations"]
        for p in pairs:
            assert p["strength"] in ("very strong", "strong", "moderate", "weak", "negligible")


class TestSingleNumericColumn:
    """Edge case: only one numeric column — can't compute correlations."""

    def test_fewer_than_two_numeric(self, sample_csv, storage_dir):
        """sample_csv has id, age, score as numeric — but let's test with column filter."""
        index_local(path=sample_csv, name="sample", storage_path=storage_dir)
        result = get_correlations(
            dataset="sample",
            columns=["id"],
            storage_path=storage_dir,
        )
        assert "error" not in result
        assert result["result"]["correlations"] == []
        assert "Need at least 2" in result["result"].get("message", "")


class TestWithSampleCSV:
    """Correlations on the standard sample fixture."""

    def test_runs_on_sample(self, indexed_sample, storage_dir):
        result = get_correlations(dataset="sample", storage_path=storage_dir)
        assert "error" not in result
        # sample has id, age, score as numeric
        assert result["result"]["numeric_columns"] >= 2
