<!-- mcp-name: io.github.jgravelle/jdatamunch-mcp -->

## FREE FOR PERSONAL USE
**Use it to make money, and Uncle J. gets a taste. Fair enough?** [details](#commercial-licenses)

---

## Cut spreadsheet token usage by **99% or more**

Most AI agents explore tabular data the expensive way:

dump the whole file into the prompt → skim a million irrelevant rows → repeat.

That is not "a little inefficient."
That is a **token incinerator**.

A 244 MB CSV file with 1 million rows costs roughly **61 million tokens** if you paste it raw.
The agent's actual question needed maybe 500 of them.

**jDataMunch indexes the file once and lets agents retrieve only the exact data they need**: column profiles, filtered rows, and server-side aggregations — with SQL precision.

In retrieval-heavy workflows, that routinely cuts data-reading token usage by **99%+** because the agent stops brute-loading giant files just to answer one narrow question.

| Task | Traditional approach | With jDataMunch |
|------|----------------------|-----------------|
| Understand a dataset | Paste entire CSV | `describe_dataset` → column names, types, cardinality, samples |
| Find relevant columns | Read every row | `search_data` → column-level results with IDs |
| Answer a filtered question | Load millions of rows | `get_rows` with structured filters → only matching rows |
| Compute a group-by | Return all data | `aggregate` → server-side SQL, one result set |

Index once. Query cheaply. Keep moving.
**Precision retrieval beats brute-force context.**

---

# jDataMunch MCP

### Structured tabular data retrieval for AI agents

![License](https://img.shields.io/badge/license-dual--use-blue)
![MCP](https://img.shields.io/badge/MCP-compatible-purple)
![Local-first](https://img.shields.io/badge/local--first-yes-brightgreen)
![SQLite](https://img.shields.io/badge/storage-SQLite-9cf)
![jMRI](https://img.shields.io/badge/jMRI-Full-blueviolet)
[![PyPI version](https://img.shields.io/pypi/v/jdatamunch-mcp)](https://pypi.org/project/jdatamunch-mcp/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/jdatamunch-mcp)](https://pypi.org/project/jdatamunch-mcp/)

> ## Commercial licenses
>
> jDataMunch-MCP is **free for non-commercial use**.
>
> **Commercial use requires a paid license.**
>
> **jDataMunch-only licenses**
>
> * [Builder — $79](https://j.gravelle.us/jDataMunch/descriptions.php#builder) — 1 developer
> * [Studio — $349](https://j.gravelle.us/jDataMunch/descriptions.php#studio) — up to 5 developers
> * [Platform — $1,999](https://j.gravelle.us/jDataMunch/descriptions.php#platform) — org-wide internal deployment
>
> **Want the full jMunch suite?**
>
> * [Munch Trio Builder Bundle — $99](https://j.gravelle.us/jDataMunch/descriptions.php#builder)
> * [Munch Trio Studio Bundle — $449](https://j.gravelle.us/jDataMunch/descriptions.php#studio)
> * [Munch Trio Platform Bundle — $2,499](https://j.gravelle.us/jDataMunch/descriptions.php#platform)

**Stop paying your model to read the whole damn spreadsheet.**

jDataMunch turns tabular data exploration into **structured retrieval**.

Instead of forcing an agent to load an entire CSV, scan millions of rows, and burn through context just to find the right column name, jDataMunch lets it navigate by **what the data is** and retrieve **only what matters**.

That means:

* **99%+ lower data-reading token usage** in retrieval-heavy workflows
* **less irrelevant context** polluting the prompt
* **faster dataset orientation** — one call tells you everything about the schema
* **accurate filtered queries** — the agent asks for Hollywood assaults, it gets Hollywood assaults
* **server-side aggregations** — GROUP BY runs in SQLite, not inside the context window

It indexes your files once using a streaming parser and SQLite, stores column profiles and row data with proper type affinity, and retrieves exactly what the agent asked for instead of re-loading the entire file on every question.

---

## Why agents need this

Most agents still handle spreadsheets like someone who prints the entire internet before reading one article:

* paste the whole CSV to answer a narrow question
* re-load the same file repeatedly across tool calls
* consume column headers, empty cells, malformed rows, and irrelevant records
* burn context window on data that was never part of the question

jDataMunch fixes that by giving them a structured way to:

* describe a dataset's schema before touching any row data
* search for the specific column that holds the answer
* retrieve only the rows that match the filter
* run aggregations server-side and get back a single result set
* orient themselves with samples before committing to a full query

Agents do not need bigger context windows.

They need **better aim**.

---

## What you get

### Column-level retrieval

Understand a dataset's full schema — types, cardinality, null rates, value distributions, samples — in a single sub-10ms call. No rows loaded.

### Filtered row retrieval

Structured filters with 10 operators (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `in`, `is_null`, `between`). All parameterized SQL — no injection surface. Hard cap of 500 rows per call to protect context budgets.

### Server-side aggregations

GROUP BY with `count`, `sum`, `avg`, `min`, `max`, `count_distinct`, `median`. The computation stays in SQLite. One compact result set comes back instead of the data the model would aggregate itself.

### Smart column search

`search_data` searches column names, value indexes, and AI summaries simultaneously. Ask for "weapon type" and get `Weapon Used Cd` back. Ask for "Hollywood" and get the column whose values contain it.

### Token savings telemetry

Every call reports `tokens_saved` and `cost_avoided` estimates. `get_session_stats` shows your cumulative savings across the session.

### Local-first speed

Indexes are stored at `~/.data-index/` by default. No cloud. No API keys required for core functionality.

---

## How it works

jDataMunch parses local CSV and Excel files using a **streaming, single-pass pipeline**:

```
CSV/Excel file
  → Streaming parser (never loads full file into memory)
  → Column profiler (type inference, cardinality, min/max/mean/median, value indexes)
  → SQLite writer (10,000-row batches, WAL mode, indexes on low-cardinality columns)
  → index.json (column profiles, stats, file hash for incremental detection)
```

When an agent queries:

```
describe_dataset  →  reads index.json in memory (< 10ms)
get_rows          →  parameterized SQL on data.sqlite (< 100ms on indexed columns)
aggregate         →  GROUP BY SQL on data.sqlite (< 200ms for simple group-by)
search_data       →  scans column profiles in memory (< 50ms)
```

**No raw file is ever re-read after the initial index.** The SQLite database serves all row-level queries.

For a 244 MB, 1-million-row CSV:
* Index time: ~30–60 seconds (one-time)
* `describe_dataset`: < 10 ms, ~500 KB response vs 244 MB source
* `get_rows` (indexed filter): < 100 ms
* Peak indexing memory: < 500 MB

---

## Start fast

### 1. Install it

```bash
pip install jdatamunch-mcp
```

For Excel (`.xlsx`) support:

```bash
pip install "jdatamunch-mcp[excel]"
```

### 2. Add it to your MCP client

If you're using Claude Code:

```bash
claude mcp add jdatamunch uvx jdatamunch-mcp
```

Or add manually to your `~/.claude.json`:

```json
{
  "mcpServers": {
    "jdatamunch-mcp": {
      "command": "uvx",
      "args": ["jdatamunch-mcp"]
    }
  }
}
```

### 3. Index a file and start querying

```
index_local(path="/path/to/data.csv", name="my-dataset")
describe_dataset(dataset="my-dataset")
get_rows(dataset="my-dataset", filters=[{"column": "City", "op": "eq", "value": "Los Angeles"}], limit=10)
```

### 4. Tell your agent to actually use it

Installing jDataMunch makes the tools available. It does **not** guarantee the agent will stop pasting entire CSVs into prompts unless you tell it to use structured retrieval first.

A simple instruction like this helps:

```markdown
Use jdatamunch-mcp for tabular data whenever available.
Always call describe_dataset first to understand the schema.
Use get_rows with filters rather than loading raw files.
Use aggregate for any group-by or summary questions.
```

---

## Tools

| Tool | What it does |
|------|-------------|
| `index_local` | Index a CSV or Excel file. Profiles columns, loads rows into SQLite. Incremental by default (skips if file unchanged). |
| `list_datasets` | List all indexed datasets with row counts, column counts, and file sizes. |
| `describe_dataset` | Full schema profile: every column's name, type, cardinality, null%, and sample values. Primary orientation tool. |
| `describe_column` | Deep profile of one column: full value distribution, histogram bins, temporal range. |
| `search_data` | Search column names and values by keyword. Returns column IDs — tells the agent where to look, not the data. |
| `get_rows` | Filtered row retrieval with 10 operators. Parameterized SQL. 500-row hard cap. |
| `aggregate` | Server-side GROUP BY: count, sum, avg, min, max, count_distinct, median. |
| `sample_rows` | Head, tail, or random sample. Good for first-look at an unfamiliar dataset. |
| `get_session_stats` | Cumulative token savings and cost avoided across the session. |

---

## Filter operators

`get_rows` and `aggregate` accept structured filters:

```json
{"column": "AREA NAME",    "op": "eq",      "value": "Hollywood"}
{"column": "Vict Age",     "op": "between", "value": [25, 35]}
{"column": "Crm Cd Desc",  "op": "contains","value": "ASSAULT"}
{"column": "Weapon Used Cd","op": "is_null","value": true}
{"column": "AREA",         "op": "in",      "value": [1, 2, 7]}
```

| Operator | Meaning |
|----------|---------|
| `eq` | equals |
| `neq` | not equals |
| `gt`, `gte` | greater than (or equal) |
| `lt`, `lte` | less than (or equal) |
| `contains` | case-insensitive substring |
| `in` | value in list |
| `is_null` | null / not null check |
| `between` | inclusive range `[min, max]` |

Multiple filters are ANDed. No raw SQL accepted — injection surface is zero.

---

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATA_INDEX_PATH` | `~/.data-index/` | Index storage location |
| `JDATAMUNCH_MAX_ROWS` | `5,000,000` | Row cap for indexing |
| `JDATAMUNCH_SHARE_SAVINGS` | `1` | Set `0` to disable anonymous token savings telemetry |
| `ANTHROPIC_API_KEY` | — | AI column summaries via Claude (v1.1+) |
| `GOOGLE_API_KEY` | — | AI column summaries via Gemini (v1.1+) |

---

## When does it help?

| Scenario | Without jDataMunch | With jDataMunch | Savings |
|----------|--------------------|-----------------|---------|
| Orient on a 244 MB CSV | Paste raw file → 61M tokens | `describe_dataset` → ~1K tokens | ~99.99% |
| Find the crime-type column | Scan headers manually | `search_data("crime type")` → column ID | structural |
| Get Hollywood assault rows | Load all 1M rows | `get_rows` with 2 filters → matching rows only | ~99%+ |
| Crime count by area | Return all rows, aggregate in LLM | `aggregate(group_by=["AREA NAME"])` → 21 rows | ~99.9% |
| Understand weapon nulls | Load column, count manually | `describe_column("Weapon Used Cd")` → `null_pct: 64.2%` | ~99.9% |
| Re-query an unchanged file | Re-load file every time | Hash check → instant skip if unchanged | 100% of re-read cost |

The case where it doesn't help: you genuinely need every row for ML training or full exports. For that, read the file directly. For everything else — exploration, filtering, aggregation, orientation — structured retrieval wins every time.

---

## ID scheme

Every column and row gets a stable ID:

```
{dataset}::{column_name}#column     →  "lapd-crime::AREA NAME#column"
{dataset}::row_{rowid}#row          →  "lapd-crime::row_4421#row"
{dataset}::{pk_col}={value}#row     →  "lapd-crime::DR_NO=211507896#row"
```

Pass column IDs directly to `describe_column`. Row IDs are returned in `get_rows` results.

---

## Part of the jMunch family

| Product | Domain | Unit of retrieval | PyPI |
|---------|--------|-------------------|------|
| [jcodemunch-mcp](https://github.com/jgravelle/jcodemunch-mcp) | Source code | Symbols (functions, classes) | `jcodemunch-mcp` |
| [jdocmunch-mcp](https://github.com/jgravelle/jdocmunch-mcp) | Documentation | Sections (headings) | `jdocmunch-mcp` |
| **jdatamunch-mcp** | **Tabular data** | **Columns, row slices, aggregations** | **`jdatamunch-mcp`** |

All three implement [jMRI](https://github.com/jgravelle/mcp-retrieval-spec) — the open retrieval interface spec. Same response envelope, same token tracking, same telemetry pattern.

---

## Best for

* analysts, finance, ops, and consultants working with large spreadsheets
* AI agents that answer questions about CSV or Excel data
* anyone paying token costs to load files they query repeatedly
* teams that want structured, auditable data access instead of raw file dumps
* developers building data-aware agents who need a drop-in retrieval layer

---

## New here?

Index a file, run `describe_dataset`, and look at what comes back.

That single call — a few milliseconds, a few hundred tokens — tells you everything that would have cost you 60 million tokens to read raw.

That's the whole idea...
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/image?repos=jgravelle/jdatamunch-mcp&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/image?repos=jgravelle/jdatamunch-mcp&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/image?repos=jgravelle/jdatamunch-mcp&type=date&legend=top-left" />
 </picture>
</a>
