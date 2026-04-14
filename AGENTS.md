# jdatamunch-mcp — AGENTS.md

> ANN-Mesh alignment file. Governs how IDE agents and Ann-layer agents operate in this repo.
> Canonical system spec lives at: https://github.com/Thelastlineofcode/ANN-Mesh/blob/main/AGENTS.md

---

## This Repo's Role in ANN-Mesh

`jdatamunch-mcp` is the **structured data ingestion MCP server** for the ANN-Mesh knowledge pipeline.
It ingests CSV, JSON, and JSONL into Neo4j (graph) and optionally Qdrant (vector).
It is the primary pipeline for natal chart data, ephemeris tables, fixed stars,
and syncretist metadata enrichment in the Levite project.

---

## Agent Ownership

| Role | Agent | Scope |
|---|---|---|
| Implementation / ops | **Scooter** (`global-developer`) | Code, PRs, fixes |
| Graph writes + schema | **Pairan** (`levite-graph-query`) | Neo4j nodes, relationships, Cypher |
| Ephemeris data prep | **Tootie** (`levite-transit`) | Prepares tabular data before ingest |
| B2B data feeds | **Ragu** (`levite-backend-dev`) | External feed ingestion to graph |
| QA / gate | **Roids** (`global-qa`) | Reviews all PRs before merge |
| Docs / changelog | **JuneBug** (`global-scribe`) | CHANGELOG.md, release notes |

**No self-merges.** Scooter opens PRs. Roids gates them.

---

## Boot Sequence (every session)

```bash
# 1. Verify rickd is live
curl http://localhost:8080/api/health

# 2. Verify Neo4j is live
curl http://localhost:7474/

# 3. Verify Qdrant (if dual-write mode)
curl http://localhost:6333/health

# 4. Hydrate agent context
ann context hydrate --agent ann-agent-scooter   # or pairan for graph ops

# 5. Pull work queue
gh issue list --repo Thelastlineofcode/jdatamunch-mcp --label agent-task
gh issue list --repo Thelastlineofcode/ANN-Mesh --label agent-task
```

---

## Execution Rules

- Load `.ann-core/skills/data-pipeline-mcp.md` (in ANN-Mesh) before operating — never guess Neo4j schema
- Always hydrate context before task: `ann context hydrate --agent [agent-id]`
- Always close loop after task: `ann context store --agent [agent-id] --task [id] --status done`
- Pairan owns all Neo4j schema decisions — confirm schema before bulk ingest
- Dual-write (Neo4j + Qdrant) requires Obi sign-off on collection name
- PRs follow `pr-authoring.md` format (conventional commits)
- Roids must approve before merge — no exceptions
- D.O. handles any security findings — no secrets in PR comments

---

## Reporting Format

```
AGENT:  [name]
TASK:   [assigned work]
STATUS: [done / blocked / in-progress]
OUTPUT: [commit SHA / PR # / node IDs / finding]
NEXT:   [what routes next and to whom]
```

---

## Key Connections

- **rickd:** `http://localhost:8080` — context hydrate/store, task dispatch
- **Neo4j:** `bolt://localhost:7687` — graph storage target
- **Qdrant:** `http://localhost:6333` — vector storage (dual-write mode)
- **ANN-Mesh:** https://github.com/Thelastlineofcode/ANN-Mesh — canonical system docs

---

*For full agent roster, routing table, and mesh spec: see ANN-Mesh AGENTS.md.*
*For MCP connection config: see CONNECT.md in this repo.*
