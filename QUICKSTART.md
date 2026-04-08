# jDataMunch Quick Start

Get from zero to 99.997% token savings in three steps.

---

## Step 1 — Install

```bash
pip install jdatamunch-mcp
```

**Optional extras** (install any you need):

| Extra | What it adds | Command |
|-------|-------------|---------|
| Excel | `.xlsx` and `.xls` support | `pip install "jdatamunch-mcp[excel]"` |
| Parquet | `.parquet` support | `pip install "jdatamunch-mcp[parquet]"` |
| Semantic | Embedding-based column search | `pip install "jdatamunch-mcp[semantic]"` |
| Everything | All of the above + AI summaries | `pip install "jdatamunch-mcp[all]"` |

> **Tip:** Use `uvx` instead of `pip install` if your MCP client has trouble finding the executable. `uvx` resolves the package on demand and avoids PATH issues.

---

## Step 2 — Add to your AI tool

Pick your client and follow the instructions below. Only do one — whichever you use.

### Claude Code

Run this in your terminal:

```bash
claude mcp add jdatamunch uvx jdatamunch-mcp
```

Restart Claude Code. Type `/mcp` to confirm it shows up.

### Claude Desktop

Open your config file:

* **Mac:** `~/Library/Application Support/Claude/claude_desktop_config.json`
* **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

Add this inside the file (or merge into your existing `mcpServers`):

```json
{
  "mcpServers": {
    "jdatamunch": {
      "command": "uvx",
      "args": ["jdatamunch-mcp"]
    }
  }
}
```

Restart Claude Desktop.

### OpenClaw

```bash
openclaw mcp set jdatamunch '{"command":"uvx","args":["jdatamunch-mcp"]}'
openclaw gateway restart
```

### Other clients (Cursor, Windsurf, Roo, etc.)

Add the same JSON block to your client's MCP configuration file. The format is the same everywhere.

---

## Step 3 — Index a file and ask questions

Tell your AI assistant to index a file:

> "Index the file at C:/Data/sales.csv and call it sales"

Behind the scenes, it runs:

```
index_local(path="C:/Data/sales.csv", name="sales")
```

This takes a few seconds to a minute depending on file size. It only happens once — after that, queries are instant.

Now ask questions naturally:

> "What columns does the sales dataset have?"

```
describe_dataset(dataset="sales")
```

> "Show me all sales over $10,000 in California"

```
get_rows(dataset="sales", filters=[
  {"column": "State", "op": "eq", "value": "California"},
  {"column": "Amount", "op": "gt", "value": 10000}
])
```

> "What's the total revenue by region?"

```
aggregate(dataset="sales", aggregations=[{"column": "Amount", "function": "sum"}], group_by=["Region"])
```

You don't need to write these tool calls yourself. Just ask your question in plain English and the AI will pick the right tool.

---

## Make your AI always use jDataMunch

By default, your AI might still try to read CSV files directly (which wastes tokens). To fix that, add a policy to your `CLAUDE.md`:

```markdown
## Data Exploration Policy
Use jdatamunch-mcp for tabular data whenever available.
Always call describe_dataset first to understand the schema.
Use get_rows with filters rather than loading raw files.
Use aggregate for any group-by or summary questions.
```

**Where to put this:**

* **Claude Code:** `~/.claude/CLAUDE.md` (global) or `CLAUDE.md` in your project folder
* **Claude Desktop:** Same as above — Claude Desktop reads CLAUDE.md files too
* **OpenClaw:** Add to your agent's system prompt file

---

## Check your savings

Ask: *"How many tokens has jDataMunch saved me?"*

Your AI calls `get_session_stats` and reports how many tokens (and dollars) you've saved compared to loading raw files.

---

## What's next?

* **Index more files** — each gets its own dataset name
* **Index from GitHub** — `index_repo(url="owner/repo")` discovers and indexes data files in a repo
* **Try semantic search** — `search_data(dataset="sales", query="customer location", semantic=true)`
* **Join datasets** — `join_datasets` combines two indexed files with SQL JOIN
* **Read the [User Manual](USER-MANUAL.md)** for the complete tool reference and real-world workflows
