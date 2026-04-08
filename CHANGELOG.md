# Changelog

## [0.6.0] — 2026-04-08

### New features

- **`get_correlations(dataset)` tool** — compute pairwise Pearson correlations between all numeric columns via SQLite. Returns pairs sorted by |r| descending with strength labels (`very strong`, `strong`, `moderate`, `weak`, `negligible`), direction, and pair counts. Configurable `min_abs_correlation` threshold (default 0.3), optional column filter, `top_n` cap (default 20, max 200). Caps at 50 numeric columns to avoid O(n^2) blowup.

### Tests

- 13 new tests (151 total, 10 skipped for optional deps)

## [0.5.0] — 2026-04-08

### New features

- **`index_repo(url)` tool** — index data files directly from a GitHub repository. Discovers CSV, Excel, Parquet, and JSONL files via the GitHub Trees API, downloads each to a temp directory, and indexes via the existing `index_local` pipeline. Datasets are named `{owner}--{repo}--{filename}`.
  - Incremental: caches HEAD SHA to skip entirely when repo is unchanged
  - Limits: 50 MB per file, 20 files per repo
  - Concurrent downloads (semaphore-limited to 5)
  - Supports `GITHUB_TOKEN` env var for private repos and rate limits

### Tests

- 18 new tests for index_repo (138 total, 10 skipped for optional deps)

## [0.4.0] — 2026-04-08

### New features

- **Natural-language summaries** — every `index_local` call now auto-generates a dataset-level summary and per-column summaries from profiled statistics. Summaries describe data shape, types, ranges, cardinality, quality issues, and temporal spans — no external API calls needed.
- **`summarize_dataset(dataset)` tool** — regenerate summaries for an already-indexed dataset without re-parsing the source file. Useful after schema or profile changes.

### Improvements

- `describe_dataset` now includes `dataset_summary` and per-column `ai_summary` fields in responses
- Column summaries surface cardinality labels (unique identifier, categorical, binary, constant, etc.), null-rate warnings, and value previews for low-cardinality columns

### Tests

- 18 new tests (120 total, 10 skipped for optional deps)

## [0.3.0] — 2026-04-01

### New tools

- **`get_schema_drift(dataset_a, dataset_b)`** — compare schema metadata between two indexed datasets: detects added/removed columns, type changes, and null-rate shifts (≥1% delta). Assessment: `identical` | `additive` | `breaking`. Pure in-memory comparison of indexed profiles — no re-reading source files.
- **`get_data_hotspots(dataset, top_n=10)`** — rank columns by composite data-quality risk combining null rate, cardinality anomalies, and numeric outlier spread (coefficient of variation). Per-column `assessment: low|medium|high`. Top-N capped at 50. Analogous to jcodemunch's `get_hotspots`.

### Tests

- 23 new tests (91 total, 1 skipped for optional deps)

## [0.2.1] — 2026-03-31

### Housekeeping

- Added `LICENSE` file (dual-use: free for non-commercial, paid for commercial)

## [0.2.0] — 2026-03-31

### New features

- **Parquet support** — `.parquet` files indexed and queried via `pyarrow`
- **JSONL/NDJSON support** — `.jsonl` and `.ndjson` files parsed line-by-line; schema inferred from first N rows
- **Token budget enforcement** (`budget.py`) — every tool response is capped at a configurable token limit (`JDATAMUNCH_MAX_RESPONSE_TOKENS`, default 8 000); falls back to generic list-field trimming when needed
- **Anti-loop call tracker** (`call_tracker.py`) — detects and warns when an LLM agent is paginating through a dataset row-by-row in a tight loop
- **Wide-table pagination** — `describe_dataset` auto-paginates at 60 columns; new `columns_offset` parameter lets callers page through remaining columns

### Improvements

- Hard caps added for all tool parameters: `top_n` ≤ 200, `histogram_bins` ≤ 50, `search_data` max_results ≤ 50, `aggregate` limit ≤ 1 000
- `get_rows` / `sample_rows` auto-project to 30 columns on wide tables; caller can override with explicit `columns` list
- `describe_dataset` tool description updated to document pagination behaviour
- `describe_column` and `search_data` tool descriptions document their caps
- Improved test fixtures (`tests/conftest.py`)

### Housekeeping

- Added `LICENSE` file (dual-use: free for non-commercial, paid for commercial)
- `index_local` description updated to list all supported formats

## [0.1.2] — 2026-03-27

### Performance

- Bulk SQLite insert, string fast-path, corrected `is_unique` detection for high-cardinality columns

## [0.1.1] — 2026-03-26

### Bug fixes

- Fixed token cost calculations in benchmark results (were off by 1 000×)

## [0.1.0] — 2026-03-25

### Initial release

- CSV and Excel (.xlsx/.xls) indexing via SQLite
- Tools: `index_local`, `list_datasets`, `describe_dataset`, `describe_column`, `search_data`, `get_rows`, `sample_rows`, `aggregate`, `get_session_stats`
- jMRI-Full compliant
