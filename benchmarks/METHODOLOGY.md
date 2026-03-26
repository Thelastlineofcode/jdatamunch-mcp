# Benchmark Methodology

This document provides full methodological detail for the token efficiency
benchmarks reported in `results.md` and the project README.

## Scope

The benchmark measures **retrieval token efficiency** — how many LLM input
tokens a data exploration tool consumes compared to reading the entire raw
source file. It does **not** measure answer quality, latency, or end-to-end
task completion.

## Dataset Under Test

| Dataset | Rows | Columns | File Size | Baseline Tokens |
|---------|-----:|--------:|----------:|----------------:|
| crime.csv (LA crime records) | 1,004,894 | 28 | 255.5 MB | 111,028,360 |

The dataset is an LAPD crime incident records CSV downloaded from the
LA Open Data portal. No filtering was applied.

## Query Corpus

Five queries chosen to represent common data exploration intents:

| Query | Intent | Key Column |
|-------|--------|-----------|
| `schema overview` | Full column schema, types, cardinality | — |
| `crime type distribution` | Top crime categories | `Crm Cd Desc` |
| `temporal range` | Date range of incidents | `DATE OCC` |
| `victim demographics` | Age/sex/descent statistics | `Vict Age` |
| `geographic coverage` | Area names, lat/lon ranges | `AREA NAME` |

## Baseline Definition

**Baseline tokens** = the entire raw CSV file read and tokenized.
This represents the **minimum** cost for a "read everything first" agent.
Real agents often read the same file multiple times, so production savings
are higher than what the benchmark reports.

## jDataMunch Workflow

For each query:
1. Call `describe_dataset()` — returns full schema profile: column types,
   cardinality, null rates, sample values, min/max/mean for numeric columns,
   date range for datetime columns, top values for categorical columns.
2. (For column-specific tasks) Call `describe_column()` on the most relevant
   column — returns deeper stats, full top-value distribution, and value index.

**Total tokens** = describe_dataset response + describe_column response.

AI summaries were **disabled** during benchmarking (`use_ai_summaries=False`).

## Token Counting Method

**Tokenizer:** `tiktoken` with `cl100k_base` encoding (used by GPT-4 and
compatible with Claude token estimates within ~5%).

Token counts are computed from the **serialized JSON response** strings,
not raw source bytes. This means:
- JSON field names and structure overhead are included (slightly understates savings).
- The count is deterministic and reproducible across runs.

### Distinction from runtime `_meta.tokens_saved`

The benchmark uses `tiktoken` for actual token counting. The runtime
`_meta.tokens_saved` field uses a byte approximation (`raw_bytes / 4`)
for zero-dependency speed. The byte approximation typically agrees within
~20% of tiktoken output for English-language CSV data. The `_meta` envelope
includes `"estimate_method": "byte_approx"` to make this explicit.

## Reproducing Results

```bash
pip install jdatamunch-mcp tiktoken

# Run the benchmark (indexes crime.csv on first run, ~45s for 1M rows)
python benchmarks/harness/run_benchmark.py /path/to/crime.csv

# Write to file
python benchmarks/harness/run_benchmark.py /path/to/crime.csv --out benchmarks/results.md
```

The harness indexes the file, tokenizes the baseline, runs each task against
`describe_dataset` + `describe_column`, and outputs markdown tables.

## Results Summary

| Dataset | Avg Reduction | Avg Ratio |
|---------|:------------:|:---------:|
| crime.csv (1M rows, 28 cols) | 100.0% | **25,333x** |

## Limitations

1. **Baseline is a lower bound.** Real agents re-read files during exploration.
   Actual baseline costs are higher.
2. **Single dataset.** Results may vary by CSV structure, column types, and
   cardinality. Well-profiled numerical or categorical data will see similar
   ratios; free-text columns provide less leverage.
3. **No quality measurement.** The benchmark assumes the describe_dataset
   response is sufficient to answer the analytical question. Answer quality
   is measured separately by
   [jMunchWorkbench](https://github.com/jgravelle/jMunchWorkbench).
4. **Single tokenizer.** Claude and GPT tokenizers produce slightly different
   counts for the same input. We use `cl100k_base` as a common reference point.

## Why the Ratio Is So Large

CSV files are extremely token-dense from an LLM perspective: every row repeats
the same value patterns, and a 1M-row file may contain >100M tokens of
essentially redundant data. `describe_dataset` distills that into a compact
schema profile (~3,800 tokens regardless of row count) by computing statistics
once during indexing. The ratio scales with row count — a 10-row CSV would
show a modest improvement; a 1M-row CSV shows a 25,000x improvement because
the schema profile stays nearly the same size.
