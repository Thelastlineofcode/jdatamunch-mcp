"""Tests for index_repo tool — GitHub remote indexing."""

import json
import os
import pytest

from jdatamunch_mcp.tools.index_repo import (
    parse_github_url,
    _should_skip,
    _discover_data_files,
    DATA_EXTENSIONS,
    MAX_FILE_SIZE,
)


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

class TestParseGitHubUrl:

    def test_owner_repo_string(self):
        assert parse_github_url("pandas-dev/pandas") == ("pandas-dev", "pandas")

    def test_https_url(self):
        assert parse_github_url("https://github.com/jgravelle/jdatamunch-mcp") == ("jgravelle", "jdatamunch-mcp")

    def test_https_url_with_git_suffix(self):
        assert parse_github_url("https://github.com/jgravelle/jdatamunch-mcp.git") == ("jgravelle", "jdatamunch-mcp")

    def test_invalid_url(self):
        with pytest.raises(ValueError, match="Could not parse"):
            parse_github_url("not-a-url")


# ---------------------------------------------------------------------------
# Skip patterns
# ---------------------------------------------------------------------------

class TestShouldSkip:

    def test_node_modules(self):
        assert _should_skip("node_modules/data/test.csv")

    def test_git_dir(self):
        assert _should_skip(".git/objects/test.csv")

    def test_normal_path(self):
        assert not _should_skip("data/sales.csv")

    def test_nested_normal(self):
        assert not _should_skip("src/fixtures/test_data.csv")


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

class TestDiscoverDataFiles:

    def _make_tree(self, entries):
        """Build a tree list from (path, size) tuples."""
        return [{"type": "blob", "path": p, "size": s} for p, s in entries]

    def test_filters_to_data_extensions(self):
        tree = self._make_tree([
            ("data/sales.csv", 1000),
            ("src/main.py", 500),
            ("README.md", 200),
            ("data/config.jsonl", 300),
        ])
        result = _discover_data_files(tree)
        paths = [f["path"] for f in result]
        assert "data/sales.csv" in paths
        assert "data/config.jsonl" in paths
        assert "src/main.py" not in paths
        assert "README.md" not in paths

    def test_skips_large_files(self):
        tree = self._make_tree([
            ("small.csv", 1000),
            ("huge.csv", MAX_FILE_SIZE + 1),
        ])
        result = _discover_data_files(tree)
        paths = [f["path"] for f in result]
        assert "small.csv" in paths
        assert "huge.csv" not in paths

    def test_skips_node_modules(self):
        tree = self._make_tree([
            ("node_modules/test/data.csv", 100),
            ("data/real.csv", 100),
        ])
        result = _discover_data_files(tree)
        paths = [f["path"] for f in result]
        assert "data/real.csv" in paths
        assert "node_modules/test/data.csv" not in paths

    def test_respects_max_files(self):
        tree = self._make_tree([
            (f"data/file{i}.csv", 100) for i in range(30)
        ])
        result = _discover_data_files(tree)
        assert len(result) == 20  # MAX_FILES

    def test_sorted_by_size(self):
        tree = self._make_tree([
            ("big.csv", 5000),
            ("small.csv", 100),
            ("medium.csv", 2000),
        ])
        result = _discover_data_files(tree)
        sizes = [f["size"] for f in result]
        assert sizes == sorted(sizes)

    def test_all_extensions_supported(self):
        tree = self._make_tree([
            ("a.csv", 100), ("b.tsv", 100), ("c.xlsx", 100),
            ("d.xls", 100), ("e.parquet", 100), ("f.jsonl", 100),
            ("g.ndjson", 100),
        ])
        result = _discover_data_files(tree)
        assert len(result) == 7

    def test_ignores_non_blob_entries(self):
        tree = [
            {"type": "tree", "path": "data", "size": 0},
            {"type": "blob", "path": "data/test.csv", "size": 100},
        ]
        result = _discover_data_files(tree)
        assert len(result) == 1

    def test_empty_tree(self):
        assert _discover_data_files([]) == []


# ---------------------------------------------------------------------------
# Integration: index_repo with mocked HTTP (offline)
# ---------------------------------------------------------------------------

class TestIndexRepoOffline:

    @pytest.mark.asyncio
    async def test_invalid_url(self):
        from jdatamunch_mcp.tools.index_repo import index_repo
        result = await index_repo(url="not-valid")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_incremental_skip_with_marker(self, tmp_path):
        """If a SHA marker matches, index_repo should skip."""
        from jdatamunch_mcp.tools.index_repo import index_repo
        storage = str(tmp_path / "store")
        os.makedirs(storage, exist_ok=True)

        # Write a marker file
        marker = tmp_path / "store" / ".repo-sha-jgravelle--jdatamunch-mcp"
        marker.write_text("abc123")

        # Mock: patch _fetch_head_sha to return the same SHA
        import unittest.mock as mock
        with mock.patch(
            "jdatamunch_mcp.tools.index_repo._fetch_head_sha",
            return_value="abc123",
        ):
            result = await index_repo(
                url="jgravelle/jdatamunch-mcp",
                incremental=True,
                storage_path=storage,
            )

        r = result.get("result", {})
        assert r.get("skipped") is True
        assert "unchanged" in r.get("reason", "").lower()
