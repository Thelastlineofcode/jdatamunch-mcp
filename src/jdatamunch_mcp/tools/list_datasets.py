"""list_datasets tool: List all indexed datasets."""

import time
from typing import Optional

from ..config import get_index_path
from ..storage.data_store import DataStore
from ..storage.token_tracker import get_total_saved


def list_datasets(storage_path: Optional[str] = None) -> dict:
    """Return summary info for all indexed datasets."""
    t0 = time.time()
    store = DataStore(base_path=storage_path or str(get_index_path()))
    datasets = store.list_datasets()
    total_saved = get_total_saved(str(store.base_path))

    return {
        "result": datasets,
        "_meta": {
            "timing_ms": round((time.time() - t0) * 1000, 1),
            "tokens_saved": 0,
            "total_tokens_saved": total_saved,
        },
    }
