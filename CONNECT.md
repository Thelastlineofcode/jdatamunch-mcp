# jdatamunch-mcp — Connection Guide

> Quick-start for IDE agents. Full guide: https://github.com/Thelastlineofcode/ANN-Mesh/blob/main/docs/JDATAMUNCH-CONNECT.md

---

## Prerequisites

```bash
# Neo4j running
curl http://localhost:7474/

# Qdrant running (dual-write mode)
curl http://localhost:6333/health

# Ollama (optional — for row embedding)
ollama list | grep nomic-embed-text

# rickd running
curl http://localhost:8080/api/health

# Python 3.11+ and uv
uv --version
```

---

## Setup

```bash
cd jdatamunch-mcp
uv sync
cp .env.example .env   # fill in values
```

`.env` minimum:
```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
QDRANT_URL=http://localhost:6333
OLLAMA_URL=http://localhost:11434
EMBED_MODEL=nomic-embed-text
RICKD_URL=http://localhost:8080
RICKD_API_KEY=rickd-dev-key
```

---

## Start Server

```bash
uv run python -m jdatamunch.server
```

---

## IDE MCP Config Block

```json
"jdatamunch": {
  "command": "uv",
  "args": ["run", "python", "-m", "jdatamunch.server"],
  "cwd": "${JDATAMUNCH_PATH}",
  "env": {
    "NEO4J_URI": "bolt://localhost:7687",
    "NEO4J_USER": "neo4j",
    "NEO4J_PASSWORD": "${NEO4J_PASSWORD}",
    "QDRANT_URL": "http://localhost:6333",
    "OLLAMA_URL": "http://localhost:11434",
    "EMBED_MODEL": "nomic-embed-text"
  }
}
```

Shell env:
```bash
export JDATAMUNCH_PATH="/path/to/jdatamunch-mcp"
export NEO4J_PASSWORD="your_password"
```

---

## Core Tool Calls

```bash
# Ingest CSV into Neo4j
mcp call jdatamunch ingest --file ./data/fixed_stars.csv --target neo4j --label FixedStar

# Ingest JSONL into Neo4j + Qdrant
mcp call jdatamunch ingest --file ./data/ephemeris.jsonl --target both --collection cosmic_data

# Write single graph node
mcp call jdatamunch write-graph --label Planet --data '{"name":"Saturn","sign":"Pisces"}'

# Query Neo4j
mcp call jdatamunch query-graph --cypher "MATCH (p:Planet) RETURN p LIMIT 10"

# Status
mcp call jdatamunch status
```

---

## Agent Rules (Summary)

- Boot: `ann health` → `ann context hydrate --agent ann-agent-pairan`
- Close loop: `ann context store` after every ingest batch
- Skill file: `.ann-core/skills/data-pipeline-mcp.md` (in ANN-Mesh repo) — load before operating
- Owner: **Pairan** (graph writes) · **Tootie** (ephemeris prep)
- Never guess Neo4j schema — check with Pairan first

*Full spec: https://github.com/Thelastlineofcode/ANN-Mesh/blob/main/docs/JDATAMUNCH-CONNECT.md*
