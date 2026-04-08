"""Tests for delete_dataset tool."""

import pytest
from pathlib import Path

from jdatamunch_mcp.tools.index_local import index_local
from jdatamunch_mcp.tools.delete_dataset import delete_dataset
from jdatamunch_mcp.tools.list_datasets import list_datasets
from jdatamunch_mcp.storage.data_store import DataStore


class TestDeleteDataset:

    def test_delete_existing(self, sample_csv, storage_dir):
        index_local(path=sample_csv, name="to-delete", storage_path=storage_dir)
        result = delete_dataset(dataset="to-delete", storage_path=storage_dir)
        assert "error" not in result
        assert result["result"]["deleted"] is True
        assert result["result"]["dataset"] == "to-delete"
        assert result["result"]["rows_removed"] == 10
        assert result["result"]["columns_removed"] == 5
        assert result["result"]["bytes_freed"] > 0

    def test_delete_removes_files(self, sample_csv, storage_dir):
        index_local(path=sample_csv, name="to-delete", storage_path=storage_dir)
        dataset_dir = Path(storage_dir) / "to-delete"
        assert dataset_dir.exists()
        assert (dataset_dir / "index.json").exists()
        assert (dataset_dir / "data.sqlite").exists()

        delete_dataset(dataset="to-delete", storage_path=storage_dir)
        assert not dataset_dir.exists()

    def test_delete_not_indexed(self, storage_dir):
        result = delete_dataset(dataset="nonexistent", storage_path=storage_dir)
        assert "error" in result
        assert "NOT_INDEXED" in result["error"]

    def test_delete_removes_from_list(self, sample_csv, storage_dir):
        index_local(path=sample_csv, name="listed", storage_path=storage_dir)
        datasets = list_datasets(storage_path=storage_dir)
        names = [d["dataset"] for d in datasets["result"]]
        assert "listed" in names

        delete_dataset(dataset="listed", storage_path=storage_dir)
        datasets = list_datasets(storage_path=storage_dir)
        names = [d["dataset"] for d in datasets["result"]]
        assert "listed" not in names

    def test_delete_then_reindex(self, sample_csv, storage_dir):
        """After deletion, the same dataset can be re-indexed."""
        index_local(path=sample_csv, name="reuse", storage_path=storage_dir)
        delete_dataset(dataset="reuse", storage_path=storage_dir)

        result = index_local(path=sample_csv, name="reuse", storage_path=storage_dir)
        assert "error" not in result
        store = DataStore(base_path=storage_dir)
        idx = store.load("reuse")
        assert idx is not None
        assert idx.row_count == 10

    def test_meta_present(self, sample_csv, storage_dir):
        index_local(path=sample_csv, name="meta-test", storage_path=storage_dir)
        result = delete_dataset(dataset="meta-test", storage_path=storage_dir)
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]
