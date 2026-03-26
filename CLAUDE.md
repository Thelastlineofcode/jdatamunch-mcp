# jdatamunch-mcp — Project Brief

## Current State
- **Version:** TBD (pre-release)
- **GitHub:** `jgravelle/jdatamunch-mcp`
- **Python:** >=3.10

## Key Files
```
src/jdatamunch_mcp/
  server.py                    # MCP tool definitions + call_tool dispatcher
  config.py                    # Index path, max rows env vars
  security.py                  # Path validation
  parser/                      # CSV/Excel file parsing
  profiler/
    column_profiler.py         # Per-column type inference, stats accumulation, finalize_profile()
  storage/
    data_store.py              # DataStore: load/save DatasetIndex (index.json)
    sqlite_store.py            # SQLite backend: create_table, insert_batch, create_indexes
    token_tracker.py           # estimate_savings, record_savings, cost_avoided
  tools/
    index_local.py             # Index a local CSV/Excel file (single-pass profiling + SQLite load)
    list_datasets.py           # List indexed datasets
    describe_dataset.py        # Full schema profile for a dataset (primary orientation tool)
    describe_column.py         # Deep stats for one column
    sample_rows.py             # Retrieve sample rows (optionally filtered)
    get_rows.py                # Retrieve rows by index range or filter
    search_data.py             # Search rows by column value / pattern
    aggregate.py               # Aggregate (count/sum/mean/min/max) with optional groupby
    get_session_stats.py       # Session token savings stats

benchmarks/
  harness/run_benchmark.py    # Token efficiency benchmark harness
  results.md                  # Latest benchmark results
  METHODOLOGY.md              # Full methodological details
```

## Benchmarks
Real production dataset (LAPD crime records, 1M rows):

| Dataset | Rows | Cols | File Size | Avg Ratio |
|---------|-----:|-----:|----------:|----------:|
| crime.csv | 1,004,894 | 28 | 255.5 MB | **25,333x** |

Baseline = full raw CSV tokenized. jDataMunch = `describe_dataset` + `describe_column`.
Benchmark harness: `python benchmarks/harness/run_benchmark.py <file.csv>`

## Architecture Notes
- `index_local` does a two-phase single pass: type inference on first 10k rows,
  then full pass for profiling + SQLite load.
- `describe_dataset` returns schema + stats from `index.json` (no SQLite query needed).
- `describe_column` returns deeper stats including full top-value distribution.
- Token tracker uses byte approximation (`raw_bytes / 4`) for zero-dependency speed.

## Ecosystem Boundary
- jdatamunch owns: CSV/Excel indexing, schema profiling, column stats, row sampling, data search
- jcodemunch owns: symbol extraction, code search (do NOT add data parsing here)
- jdocmunch owns: doc section search (do NOT add data parsing here)
