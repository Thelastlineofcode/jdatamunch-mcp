"""DataIndex dataclass and DataStore: save/load/list/delete dataset indexes.

Storage layout:
    ~/.data-index/
        {dataset_id}/
            index.json      — DataIndex: profiles, stats, schema
            data.sqlite     — Row-level data for filtered retrieval
        _savings.json       — Token savings tracker
"""

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

INDEX_VERSION = 1


@dataclass
class DataIndex:
    """Index for a single tabular dataset."""
    dataset: str
    source_path: str
    source_format: str       # "csv", "xlsx", etc.
    source_hash: str         # SHA-256 of source file
    source_size_bytes: int
    indexed_at: str          # ISO datetime string
    index_version: int
    row_count: int
    column_count: int
    encoding: str
    delimiter: str
    columns: list            # list of column profile dicts (serialized ColumnProfile)
    sqlite_relative_path: str = "data.sqlite"
    dataset_summary: Optional[str] = None


def _hash_file(path: str) -> str:
    """Compute SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def _profile_to_dict(p: Any) -> dict:
    """Serialize a ColumnProfile dataclass to a JSON-safe dict."""
    return {
        "name": p.name,
        "position": p.position,
        "type": p.type,
        "count": p.count,
        "null_count": p.null_count,
        "null_pct": p.null_pct,
        "cardinality": p.cardinality,
        "cardinality_is_exact": p.cardinality_is_exact,
        "is_unique": p.is_unique,
        "is_primary_key_candidate": p.is_primary_key_candidate,
        "min": p.min,
        "max": p.max,
        "mean": p.mean,
        "median": p.median,
        "sample_values": p.sample_values,
        "value_index": p.value_index,
        "top_values": p.top_values,
        "datetime_min": p.datetime_min,
        "datetime_max": p.datetime_max,
        "datetime_format": p.datetime_format,
        "ai_summary": p.ai_summary,
    }


def _index_to_dict(idx: DataIndex) -> dict:
    return {
        "dataset": idx.dataset,
        "source_path": idx.source_path,
        "source_format": idx.source_format,
        "source_hash": idx.source_hash,
        "source_size_bytes": idx.source_size_bytes,
        "indexed_at": idx.indexed_at,
        "index_version": idx.index_version,
        "row_count": idx.row_count,
        "column_count": idx.column_count,
        "encoding": idx.encoding,
        "delimiter": idx.delimiter,
        "columns": idx.columns,  # already dicts
        "sqlite_relative_path": idx.sqlite_relative_path,
        "dataset_summary": idx.dataset_summary,
    }


def _index_from_dict(d: dict) -> DataIndex:
    return DataIndex(
        dataset=d["dataset"],
        source_path=d["source_path"],
        source_format=d["source_format"],
        source_hash=d["source_hash"],
        source_size_bytes=d["source_size_bytes"],
        indexed_at=d["indexed_at"],
        index_version=d.get("index_version", 1),
        row_count=d["row_count"],
        column_count=d["column_count"],
        encoding=d.get("encoding", "utf-8"),
        delimiter=d.get("delimiter", ","),
        columns=d.get("columns", []),
        sqlite_relative_path=d.get("sqlite_relative_path", "data.sqlite"),
        dataset_summary=d.get("dataset_summary"),
    )


class DataStore:
    """Storage for dataset indexes with helpers for all CRUD operations."""

    def __init__(self, base_path: Optional[str] = None):
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = Path.home() / ".data-index"
        self.base_path.mkdir(parents=True, exist_ok=True)

    def dataset_dir(self, dataset_id: str) -> Path:
        return self.base_path / dataset_id

    def index_path(self, dataset_id: str) -> Path:
        return self.dataset_dir(dataset_id) / "index.json"

    def sqlite_path(self, dataset_id: str) -> Path:
        return self.dataset_dir(dataset_id) / "data.sqlite"

    def save(
        self,
        dataset_id: str,
        profiles: list,   # list[ColumnProfile]
        source_path: str,
        source_format: str,
        row_count: int,
        encoding: str,
        delimiter: str,
        dataset_summary: Optional[str] = None,
    ) -> DataIndex:
        """Build and persist a DataIndex from profiling results."""
        source_hash = _hash_file(source_path)
        source_size = Path(source_path).stat().st_size

        column_dicts = [_profile_to_dict(p) for p in profiles]

        idx = DataIndex(
            dataset=dataset_id,
            source_path=str(source_path),
            source_format=source_format,
            source_hash=source_hash,
            source_size_bytes=source_size,
            indexed_at=datetime.now().isoformat(),
            index_version=INDEX_VERSION,
            row_count=row_count,
            column_count=len(profiles),
            encoding=encoding,
            delimiter=delimiter,
            columns=column_dicts,
            dataset_summary=dataset_summary,
        )

        dir_ = self.dataset_dir(dataset_id)
        dir_.mkdir(parents=True, exist_ok=True)
        path = self.index_path(dataset_id)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_index_to_dict(idx), f, indent=2)
        tmp.replace(path)

        return idx

    def load(self, dataset_id: str) -> Optional[DataIndex]:
        """Load a DataIndex from storage. Returns None if not found."""
        path = self.index_path(dataset_id)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return None

        if data.get("index_version", 1) != INDEX_VERSION:
            return None  # version mismatch → triggers full re-index

        return _index_from_dict(data)

    def needs_reindex(self, dataset_id: str, source_path: str) -> bool:
        """Return True if the source file has changed or was never indexed."""
        idx = self.load(dataset_id)
        if idx is None:
            return True
        current_hash = _hash_file(source_path)
        return idx.source_hash != current_hash

    def list_datasets(self) -> list:
        """Return summary info for all indexed datasets."""
        result = []
        for subdir in sorted(self.base_path.iterdir()):
            if not subdir.is_dir() or subdir.name.startswith("_"):
                continue
            index_file = subdir / "index.json"
            if not index_file.exists():
                continue
            try:
                with open(index_file, "r", encoding="utf-8") as f:
                    d = json.load(f)
                result.append({
                    "dataset": d["dataset"],
                    "file": Path(d["source_path"]).name,
                    "rows": d["row_count"],
                    "columns": d["column_count"],
                    "size_bytes": d["source_size_bytes"],
                    "source_format": d.get("source_format", "csv"),
                    "indexed_at": d["indexed_at"],
                })
            except Exception:
                continue
        return result

    def delete(self, dataset_id: str) -> bool:
        """Delete a dataset index and its SQLite file."""
        dir_ = self.dataset_dir(dataset_id)
        if dir_.exists():
            shutil.rmtree(dir_)
            return True
        return False
