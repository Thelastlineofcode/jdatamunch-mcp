"""delete_dataset tool: Remove an indexed dataset and its SQLite store."""

import time
from typing import Optional

from ..config import get_index_path
from ..storage.data_store import DataStore


def delete_dataset(
    dataset: str,
    storage_path: Optional[str] = None,
) -> dict:
    """Delete an indexed dataset (index.json + data.sqlite).

    Returns confirmation with the dataset name and size freed.
    """
    t0 = time.time()
    store = DataStore(base_path=storage_path or str(get_index_path()))

    idx = store.load(dataset)
    if idx is None:
        return {"error": f"NOT_INDEXED: dataset {dataset!r} is not indexed."}

    # Gather stats before deletion
    dataset_dir = store.dataset_dir(dataset)
    bytes_freed = sum(f.stat().st_size for f in dataset_dir.rglob("*") if f.is_file())

    deleted = store.delete(dataset)
    if not deleted:
        return {"error": f"DELETE_FAILED: could not remove {dataset!r}."}

    return {
        "result": {
            "dataset": dataset,
            "deleted": True,
            "rows_removed": idx.row_count,
            "columns_removed": idx.column_count,
            "bytes_freed": bytes_freed,
        },
        "_meta": {
            "timing_ms": round((time.time() - t0) * 1000, 1),
        },
    }
