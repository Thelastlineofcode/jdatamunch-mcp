"""MCP server for jdatamunch-mcp."""

import argparse
import asyncio
import json
import os
import sys
import traceback
from typing import Optional

from mcp.server import Server
from mcp.types import Tool, TextContent, Resource

from .tools.index_local import index_local
from .tools.list_datasets import list_datasets
from .tools.describe_dataset import describe_dataset
from .tools.describe_column import describe_column
from .tools.search_data import search_data
from .tools.get_rows import get_rows
from .tools.aggregate import aggregate
from .tools.sample_rows import sample_rows
from .tools.get_session_stats import get_session_stats
from .tools.get_schema_drift import get_schema_drift
from .tools.get_data_hotspots import get_data_hotspots
from .tools.summarize_dataset import summarize_dataset as summarize_dataset_tool
from .tools.index_repo import index_repo
from .tools.get_correlations import get_correlations
from .budget import enforce_budget
from .call_tracker import record_call

server = Server("jdatamunch-mcp")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List all available tools."""
    return [
        Tool(
            name="index_local",
            description=(
                "Index a local data file (CSV, Excel, Parquet, or JSONL). Profiles all columns, "
                "detects types, computes statistics, and loads rows into SQLite for fast filtered "
                "retrieval. Set incremental=true (default) to skip re-indexing if file is unchanged."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute path to data file (.csv, .tsv, .xlsx, .xls, .parquet, .jsonl, .ndjson)",
                    },
                    "name": {
                        "type": "string",
                        "description": "Dataset identifier override (defaults to filename stem)",
                    },
                    "incremental": {
                        "type": "boolean",
                        "description": "Skip re-index if file hash unchanged (default true)",
                        "default": True,
                    },
                    "encoding": {
                        "type": "string",
                        "description": "File encoding override (auto-detected if omitted)",
                    },
                    "delimiter": {
                        "type": "string",
                        "description": "CSV delimiter override (auto-detected if omitted)",
                    },
                    "header_row": {
                        "type": "integer",
                        "description": "Row number containing column headers, 0-indexed (default 0)",
                        "default": 0,
                    },
                    "sheet": {
                        "type": "string",
                        "description": "Excel sheet name to index (default: first sheet)",
                    },
                },
                "required": ["path"],
            },
        ),
        Tool(
            name="index_repo",
            description=(
                "Index data files from a GitHub repository. Discovers CSV, Excel, Parquet, "
                "and JSONL files, downloads them, and indexes each via the same pipeline as "
                "index_local. Datasets are named {owner}--{repo}--{filename}. "
                "Max 50 MB per file, 20 files per repo. Set GITHUB_TOKEN env var for "
                "private repos or to avoid rate limits."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "GitHub repo URL or owner/repo string (e.g. 'pandas-dev/pandas' or 'https://github.com/pandas-dev/pandas')",
                    },
                    "incremental": {
                        "type": "boolean",
                        "description": "Skip re-index if HEAD SHA unchanged (default true)",
                        "default": True,
                    },
                    "github_token": {
                        "type": "string",
                        "description": "GitHub token override (defaults to GITHUB_TOKEN env var)",
                    },
                },
                "required": ["url"],
            },
        ),
        Tool(
            name="list_datasets",
            description="List all indexed datasets with summary statistics.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="describe_dataset",
            description=(
                "Primary orientation tool. Returns every column's name, type, cardinality, "
                "null%, and sample values. A single call replaces reading the entire source file. "
                "Equivalent to opening a spreadsheet and reading the column headers + stats. "
                "On wide tables (60+ columns), results are auto-paginated — use columns=[] to "
                "select specific ones, or columns_offset to page through remaining columns."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset identifier (from list_datasets or index_local)",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Filter to specific columns (default: all)",
                    },
                    "columns_offset": {
                        "type": "integer",
                        "description": "Pagination offset for wide tables (default 0)",
                        "default": 0,
                    },
                },
                "required": ["dataset"],
            },
        ),
        Tool(
            name="describe_column",
            description=(
                "Deep profile of a single column. Full value distribution for low-cardinality "
                "columns, histogram bins for numeric, temporal range for datetime. "
                "top_n capped at 200; histogram_bins capped at 50."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "Dataset identifier"},
                    "column": {
                        "type": "string",
                        "description": "Column name or column ID (e.g. 'lapd-crime::AREA NAME#column')",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Top values to return for categorical columns (default 20)",
                        "default": 20,
                    },
                    "histogram_bins": {
                        "type": "integer",
                        "description": "Bins for numeric histograms (default 10)",
                        "default": 10,
                    },
                },
                "required": ["dataset", "column"],
            },
        ),
        Tool(
            name="search_data",
            description=(
                "Search across column names and values. Returns column-level results with IDs "
                "— tells you where to look, not the data itself. Use before get_rows or describe_column. "
                "max_results capped at 50."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "Dataset identifier"},
                    "query": {
                        "type": "string",
                        "description": "Natural-language or keyword query",
                    },
                    "search_scope": {
                        "type": "string",
                        "enum": ["all", "schema", "values"],
                        "description": "Limit search to schema only, values only, or all (default 'all')",
                        "default": "all",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum results to return (default 10)",
                        "default": 10,
                    },
                },
                "required": ["dataset", "query"],
            },
        ),
        Tool(
            name="get_rows",
            description=(
                "Filtered row retrieval via structured filters. All filters are SQL-parameterized "
                "(no injection). Operators: eq, neq, gt, gte, lt, lte, contains, in, is_null, between. "
                "Use columns=[] to project — reduces tokens significantly on wide tables. "
                "Prefer aggregate() for summaries over paginating through rows."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "Dataset identifier"},
                    "filters": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "op": {
                                    "type": "string",
                                    "enum": ["eq", "neq", "gt", "gte", "lt", "lte",
                                             "contains", "in", "is_null", "between"],
                                },
                                "value": {},
                            },
                            "required": ["column", "op"],
                        },
                        "description": "Filter conditions (ANDed). E.g. [{\"column\": \"AREA NAME\", \"op\": \"eq\", \"value\": \"Hollywood\"}]",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column projection — reduces tokens (default: all)",
                    },
                    "order_by": {"type": "string", "description": "Column to sort by"},
                    "order_dir": {
                        "type": "string",
                        "enum": ["asc", "desc"],
                        "description": "Sort direction (default 'asc')",
                        "default": "asc",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows returned (default 50, hard cap 500)",
                        "default": 50,
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Pagination offset (default 0)",
                        "default": 0,
                    },
                },
                "required": ["dataset"],
            },
        ),
        Tool(
            name="aggregate",
            description=(
                "Server-side aggregations (GROUP BY). Saves orders of magnitude in tokens "
                "vs returning rows for the LLM to aggregate. Functions: count, sum, avg, "
                "min, max, count_distinct, median. limit capped at 1000."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "Dataset identifier"},
                    "aggregations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "function": {
                                    "type": "string",
                                    "enum": ["count", "sum", "avg", "min", "max",
                                             "count_distinct", "median"],
                                },
                                "alias": {"type": "string"},
                            },
                            "required": ["column", "function"],
                        },
                        "description": "Aggregation specs. Use column='*' for COUNT(*).",
                    },
                    "group_by": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Group-by columns. Empty = whole-dataset aggregate.",
                    },
                    "filters": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Pre-filter rows before aggregating (same syntax as get_rows)",
                    },
                    "order_by": {"type": "string", "description": "Column or alias to sort by"},
                    "order_dir": {
                        "type": "string",
                        "enum": ["asc", "desc"],
                        "default": "desc",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max groups returned (default 50)",
                        "default": 50,
                    },
                },
                "required": ["dataset", "aggregations"],
            },
        ),
        Tool(
            name="sample_rows",
            description=(
                "Return a sample of rows. Useful for understanding data shape "
                "without prior knowledge. Method: 'head', 'tail', or 'random'. "
                "Use columns=[] on wide tables to reduce response size."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {"type": "string", "description": "Dataset identifier"},
                    "n": {
                        "type": "integer",
                        "description": "Rows to sample (default 5, max 100)",
                        "default": 5,
                    },
                    "method": {
                        "type": "string",
                        "enum": ["head", "tail", "random"],
                        "description": "Sampling method (default 'head')",
                        "default": "head",
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Column projection (default: all)",
                    },
                },
                "required": ["dataset"],
            },
        ),
        Tool(
            name="get_schema_drift",
            description=(
                "Compare schema (columns, types, nullability) between two indexed datasets. "
                "Detects added/removed columns, type changes, and null-rate shifts. "
                "Pure in-memory comparison — no re-reading source files. "
                "Useful for detecting schema changes between dataset versions. "
                "Assessment: 'identical' | 'additive' (only additions) | 'breaking' (removals or type changes)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_a": {
                        "type": "string",
                        "description": "First dataset identifier (baseline)",
                    },
                    "dataset_b": {
                        "type": "string",
                        "description": "Second dataset identifier (comparison target)",
                    },
                },
                "required": ["dataset_a", "dataset_b"],
            },
        ),
        Tool(
            name="get_data_hotspots",
            description=(
                "Return the highest-risk columns in a dataset ranked by a composite score "
                "combining: null rate, cardinality anomalies, and numeric outlier spread. "
                "Use this as a first-look triage — analogous to jcodemunch's get_hotspots. "
                "top_n capped at 50. Assessment per column: 'low' | 'medium' | 'high'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset identifier",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of hotspot columns to return (default 10, max 50)",
                        "default": 10,
                    },
                },
                "required": ["dataset"],
            },
        ),
        Tool(
            name="get_correlations",
            description=(
                "Compute pairwise Pearson correlations between numeric columns. "
                "Returns pairs sorted by |r| descending, filtered to significant correlations. "
                "Use this to discover relationships in the data without manual exploration. "
                "top_n capped at 200."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset identifier",
                    },
                    "min_abs_correlation": {
                        "type": "number",
                        "description": "Minimum |r| to include in results (default 0.3)",
                        "default": 0.3,
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Restrict to specific numeric columns (default: all numeric)",
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Max pairs to return (default 20, max 200)",
                        "default": 20,
                    },
                },
                "required": ["dataset"],
            },
        ),
        Tool(
            name="summarize_dataset",
            description=(
                "Generate natural-language summaries for a dataset and all its columns. "
                "Works on already-indexed datasets — reads profiles from index.json, "
                "generates summaries, and writes them back. No re-parsing of source files. "
                "Summaries are also auto-generated during index_local."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset identifier (from list_datasets)",
                    },
                },
                "required": ["dataset"],
            },
        ),
        Tool(
            name="get_session_stats",
            description="Return cumulative token savings and cost avoided across all tool calls.",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.list_resources()
async def list_resources() -> list[Resource]:
    """Return empty resource list for client compatibility."""
    return []


@server.list_prompts()
async def list_prompts() -> list:
    """Return empty prompt list for client compatibility."""
    return []


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Dispatch tool calls to implementations."""
    storage_path = os.environ.get("DATA_INDEX_PATH")

    try:
        # Anti-loop detection for row-retrieval tools
        dataset_arg = arguments.get("dataset", "")
        if name in ("get_rows", "sample_rows", "aggregate", "search_data", "describe_dataset"):
            loop_warning = record_call(
                tool=name,
                dataset=dataset_arg,
                offset=arguments.get("offset", 0),
            )
        else:
            loop_warning = None

        if name == "index_local":
            result = await asyncio.to_thread(
                index_local,
                path=arguments["path"],
                name=arguments.get("name"),
                incremental=arguments.get("incremental", True),
                encoding=arguments.get("encoding"),
                delimiter=arguments.get("delimiter"),
                header_row=arguments.get("header_row", 0),
                sheet=arguments.get("sheet"),
                use_ai_summaries=arguments.get("use_ai_summaries", True),
                storage_path=storage_path,
            )
        elif name == "index_repo":
            result = await index_repo(
                url=arguments["url"],
                incremental=arguments.get("incremental", True),
                github_token=arguments.get("github_token"),
                storage_path=storage_path,
            )
        elif name == "list_datasets":
            result = list_datasets(storage_path=storage_path)

        elif name == "describe_dataset":
            result = describe_dataset(
                dataset=arguments["dataset"],
                columns=arguments.get("columns"),
                columns_offset=arguments.get("columns_offset", 0),
                storage_path=storage_path,
            )
        elif name == "describe_column":
            result = describe_column(
                dataset=arguments["dataset"],
                column=arguments["column"],
                top_n=arguments.get("top_n", 20),
                histogram_bins=arguments.get("histogram_bins", 10),
                storage_path=storage_path,
            )
        elif name == "search_data":
            result = search_data(
                dataset=arguments["dataset"],
                query=arguments["query"],
                search_scope=arguments.get("search_scope", "all"),
                max_results=arguments.get("max_results", 10),
                storage_path=storage_path,
            )
        elif name == "get_rows":
            result = await asyncio.to_thread(
                get_rows,
                dataset=arguments["dataset"],
                filters=arguments.get("filters"),
                columns=arguments.get("columns"),
                order_by=arguments.get("order_by"),
                order_dir=arguments.get("order_dir", "asc"),
                limit=arguments.get("limit", 50),
                offset=arguments.get("offset", 0),
                storage_path=storage_path,
            )
        elif name == "aggregate":
            result = await asyncio.to_thread(
                aggregate,
                dataset=arguments["dataset"],
                aggregations=arguments["aggregations"],
                group_by=arguments.get("group_by"),
                filters=arguments.get("filters"),
                order_by=arguments.get("order_by"),
                order_dir=arguments.get("order_dir", "desc"),
                limit=arguments.get("limit", 50),
                storage_path=storage_path,
            )
        elif name == "sample_rows":
            result = await asyncio.to_thread(
                sample_rows,
                dataset=arguments["dataset"],
                n=arguments.get("n", 5),
                method=arguments.get("method", "head"),
                columns=arguments.get("columns"),
                storage_path=storage_path,
            )
        elif name == "get_session_stats":
            result = get_session_stats(storage_path=storage_path)
        elif name == "get_schema_drift":
            result = get_schema_drift(
                dataset_a=arguments["dataset_a"],
                dataset_b=arguments["dataset_b"],
                storage_path=storage_path,
            )
        elif name == "get_data_hotspots":
            result = get_data_hotspots(
                dataset=arguments["dataset"],
                top_n=arguments.get("top_n", 10),
                storage_path=storage_path,
            )
        elif name == "get_correlations":
            result = await asyncio.to_thread(
                get_correlations,
                dataset=arguments["dataset"],
                min_abs_correlation=arguments.get("min_abs_correlation", 0.3),
                columns=arguments.get("columns"),
                top_n=arguments.get("top_n", 20),
                storage_path=storage_path,
            )
        elif name == "summarize_dataset":
            result = summarize_dataset_tool(
                dataset=arguments["dataset"],
                storage_path=storage_path,
            )
        else:
            result = {"error": f"Unknown tool: {name}"}

        if isinstance(result, dict) and "error" not in result:
            result = enforce_budget(result, name)
            if loop_warning:
                result.setdefault("_meta", {})["loop_warning"] = loop_warning

        if isinstance(result, dict):
            result.setdefault("_meta", {})["powered_by"] = (
                "jdatamunch-mcp by jgravelle · https://github.com/jgravelle/jdatamunch-mcp"
            )

        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


async def run_server():
    """Run the MCP server."""
    from jdatamunch_mcp import __version__
    from mcp.server.stdio import stdio_server

    print(
        f"jdatamunch-mcp {__version__} by jgravelle · https://github.com/jgravelle/jdatamunch-mcp",
        file=sys.stderr,
    )
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main(argv: Optional[list] = None):
    """Main entry point."""
    from .security import verify_package_integrity
    verify_package_integrity()

    parser = argparse.ArgumentParser(
        prog="jdatamunch-mcp",
        description="Run the jDataMunch MCP stdio server.",
    )
    parser.parse_args(argv)
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
