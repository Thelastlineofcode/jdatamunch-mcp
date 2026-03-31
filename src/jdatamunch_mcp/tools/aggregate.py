"""aggregate tool: Server-side aggregations (GROUP BY) — v1.1.0."""

import json
import time
from typing import Optional

from ..config import get_index_path, HARD_CAP_AGGREGATE_LIMIT
from ..security import validate_filter
from ..storage.data_store import DataStore
from ..storage.sqlite_store import query_aggregate
from ..storage.token_tracker import estimate_savings, record_savings, cost_avoided


def aggregate(
    dataset: str,
    aggregations: list,
    group_by: Optional[list] = None,
    filters: Optional[list] = None,
    order_by: Optional[str] = None,
    order_dir: str = "desc",
    limit: int = 50,
    storage_path: Optional[str] = None,
) -> dict:
    """Compute server-side aggregations on a dataset.

    Saves orders of magnitude in tokens vs returning rows for the LLM to aggregate.
    Aggregation functions: count, sum, avg, min, max, count_distinct, median.
    """
    t0 = time.time()
    limit = min(max(1, limit), HARD_CAP_AGGREGATE_LIMIT)

    if not aggregations:
        return {"error": "INVALID_FILTER: aggregations list is required"}

    store = DataStore(base_path=storage_path or str(get_index_path()))
    idx = store.load(dataset)
    if idx is None:
        return {"error": f"NOT_INDEXED: dataset {dataset!r} is not indexed."}

    sqlite_path = store.sqlite_path(dataset)
    if not sqlite_path.exists():
        return {"error": f"NOT_INDEXED: SQLite database missing for {dataset!r}. Re-index."}

    schema_cols = idx.columns

    # Validate filters
    if filters:
        for f in filters:
            try:
                validate_filter(f, schema_cols)
            except ValueError as e:
                return {"error": str(e)}

    try:
        agg_result = query_aggregate(
            sqlite_path=sqlite_path,
            schema_columns=schema_cols,
            group_by=group_by,
            aggregations=aggregations,
            filters=filters,
            order_by=order_by,
            order_dir=order_dir,
            limit=limit,
        )
    except ValueError as e:
        return {"error": str(e)}

    response_bytes = len(json.dumps(agg_result).encode("utf-8"))
    tokens_saved = estimate_savings(idx.source_size_bytes, response_bytes)
    total_saved = record_savings(tokens_saved, str(store.base_path))

    return {
        "result": agg_result,
        "_meta": {
            "timing_ms": round((time.time() - t0) * 1000, 1),
            "tokens_saved": tokens_saved,
            "total_tokens_saved": total_saved,
            **cost_avoided(tokens_saved, total_saved),
        },
    }
