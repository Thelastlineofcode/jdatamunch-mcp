"""index_local tool: Index a local CSV/Excel file into SQLite + index.json."""

import time
from pathlib import Path
from typing import Optional

from ..config import get_index_path, get_max_rows
from ..parser import parse_file
from ..profiler.column_profiler import _ColAcc, update_acc, finalize_profile, infer_types_from_sample, _TYPE_FROM_RANK
from ..storage.data_store import DataStore
from ..storage.sqlite_store import create_table, insert_batch, create_indexes, BATCH_SIZE
from ..storage.token_tracker import record_savings, estimate_savings

_TYPE_SAMPLE_ROWS = 10_000  # rows used for preliminary type detection


def index_local(
    path: str,
    name: Optional[str] = None,
    incremental: bool = True,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
    header_row: int = 0,
    sheet: Optional[str] = None,
    use_ai_summaries: bool = True,
    storage_path: Optional[str] = None,
) -> dict:
    """Index a local CSV or Excel file.

    Pipeline:
    1. Detect encoding + parse header
    2. Read first 10k rows for type inference
    3. Create SQLite table with inferred column types
    4. Single pass over ALL rows: update profiling accumulators + insert into SQLite
    5. Finalize column profiles
    6. Create SQLite indexes on low-cardinality columns
    7. Save index.json
    """
    t0 = time.time()
    store = DataStore(base_path=storage_path or str(get_index_path()))

    # Derive dataset ID from name or filename
    p = Path(path)
    dataset_id = name or p.stem.lower().replace(" ", "-")

    # Incremental: skip if file hash unchanged
    if incremental and not store.needs_reindex(dataset_id, str(p)):
        idx = store.load(dataset_id)
        return {
            "result": {
                "dataset": dataset_id,
                "skipped": True,
                "reason": "file unchanged (incremental=true)",
                "rows": idx.row_count if idx else 0,
                "columns": idx.column_count if idx else 0,
                "indexed_at": idx.indexed_at if idx else None,
            },
            "_meta": {
                "timing_ms": round((time.time() - t0) * 1000, 1),
                "tokens_saved": 0,
                "total_tokens_saved": 0,
            },
        }

    # Validate file exists and format is supported before expensive work
    try:
        p = p.resolve(strict=True)
    except (FileNotFoundError, OSError) as e:
        return {"error": f"INDEX_ERROR: {e}"}

    # Parse file
    try:
        parsed = parse_file(
            path=str(p),
            encoding=encoding,
            delimiter=delimiter,
            header_row=header_row,
            sheet=sheet,
        )
    except (ValueError, FileNotFoundError, OSError) as e:
        return {"error": f"INDEX_ERROR: {e}"}

    columns = parsed.columns
    n_cols = len(columns)
    meta = parsed.metadata
    source_format = p.suffix.lower().lstrip(".")

    # --- Phase 1: Read sample rows for type detection ---
    sample_rows: list = []
    row_iter = parsed.row_iterator
    max_rows = get_max_rows()

    for row in row_iter:
        sample_rows.append(row)
        if len(sample_rows) >= _TYPE_SAMPLE_ROWS:
            break

    # Build per-column accumulators and run type inference on sample
    accs = [_ColAcc(name=col.name, position=col.position) for col in columns]
    infer_types_from_sample(accs, sample_rows)

    # Preliminary types (may be promoted during full pass)
    preliminary_types = [_TYPE_FROM_RANK[acc.type_rank] for acc in accs]
    column_names = [col.name for col in columns]

    # --- Phase 2: Create SQLite schema ---
    sqlite_path = store.sqlite_path(dataset_id)
    create_table(sqlite_path, column_names, preliminary_types)

    # --- Phase 3: Full single pass — profile + load SQLite ---
    row_count = 0
    batch: list = []

    # Process sample rows first (they were already profiled in sample pass)
    # but still need inserting into SQLite
    for row in sample_rows:
        row_count += 1
        batch.append(row)
        if row_count >= max_rows:
            break
        if len(batch) >= BATCH_SIZE:
            insert_batch(sqlite_path, batch, column_names, preliminary_types)
            batch = []

    # Continue with remaining rows (not in sample)
    if row_count < max_rows:
        for row in row_iter:
            row_count += 1
            # Update profiling accumulators
            for i, acc in enumerate(accs):
                raw = row[i] if i < len(row) else ""
                update_acc(acc, raw)

            batch.append(row)
            if row_count >= max_rows:
                break
            if len(batch) >= BATCH_SIZE:
                insert_batch(sqlite_path, batch, column_names, preliminary_types)
                batch = []

    # Flush remaining batch
    if batch:
        insert_batch(sqlite_path, batch, column_names, preliminary_types)

    # --- Phase 4: Finalize profiles ---
    profiles = [finalize_profile(acc) for acc in accs]

    # --- Phase 5: Create SQLite indexes on low-cardinality columns ---
    create_indexes(sqlite_path, profiles)

    # --- Phase 6: Save index.json ---
    idx = store.save(
        dataset_id=dataset_id,
        profiles=profiles,
        source_path=str(p.resolve()),
        source_format=source_format,
        row_count=row_count,
        encoding=meta.get("encoding", "utf-8"),
        delimiter=meta.get("delimiter", ","),
        dataset_summary=None,  # v1.1: AI summaries
    )

    duration_s = time.time() - t0

    # Token savings: raw file size vs index.json size
    index_size = store.index_path(dataset_id).stat().st_size
    tokens_saved = estimate_savings(meta.get("file_size", 0), index_size)
    total_saved = record_savings(tokens_saved, str(store.base_path))

    # Column type summary
    type_counts: dict = {}
    for p_ in profiles:
        type_counts[p_.type] = type_counts.get(p_.type, 0) + 1

    return {
        "result": {
            "dataset": dataset_id,
            "file": p.name,
            "rows": row_count,
            "columns": n_cols,
            "size_bytes": meta.get("file_size", 0),
            "column_types": type_counts,
            "indexed_at": idx.indexed_at,
            "duration_seconds": round(duration_s, 1),
        },
        "_meta": {
            "timing_ms": round(duration_s * 1000, 1),
            "tokens_saved": tokens_saved,
            "total_tokens_saved": total_saved,
        },
    }
