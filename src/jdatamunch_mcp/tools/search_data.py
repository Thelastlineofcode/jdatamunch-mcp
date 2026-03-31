"""search_data tool: Search across column names, values, and metadata."""

import time
from typing import Optional

from ..config import get_index_path, HARD_CAP_SEARCH_MAX_RESULTS
from ..storage.data_store import DataStore
from ..storage.token_tracker import get_total_saved

# Scoring weights (per PRD)
_W_NAME_EXACT = 20
_W_NAME_SUBSTR = 10
_W_NAME_WORD = 5
_W_AI_SUMMARY_WORD = 3
_W_VALUE_EXACT = 8
_W_VALUE_SUBSTR = 4
_W_TYPE_BOOST = 2

_DATE_KEYWORDS = frozenset(["date", "time", "year", "month", "day", "datetime", "timestamp"])
_NUM_KEYWORDS = frozenset(["count", "amount", "number", "num", "total", "age", "id", "code"])


def _score_column(col: dict, query_lower: str, query_words: set) -> tuple:
    """Score a column against a query. Returns (score, match_details)."""
    score = 0
    matched_values: list = []
    match_type = "schema"

    name_lower = col["name"].lower()

    # Column name scoring
    if query_lower == name_lower:
        score += _W_NAME_EXACT
    elif query_lower in name_lower:
        score += _W_NAME_SUBSTR
    else:
        name_words = set(name_lower.replace("_", " ").replace("-", " ").split())
        word_hits = len(query_words & name_words)
        if word_hits:
            score += word_hits * _W_NAME_WORD

    # AI summary scoring (when available)
    if col.get("ai_summary"):
        summary_lower = col["ai_summary"].lower()
        for word in query_words:
            if word in summary_lower:
                score += _W_AI_SUMMARY_WORD

    # Value index: exact match
    value_source: list = []
    if col.get("value_index"):
        value_source = list(col["value_index"].keys())
    elif col.get("top_values"):
        value_source = [tv["value"] for tv in col["top_values"]]

    for v in value_source:
        v_lower = str(v).lower()
        hit = False
        for word in query_words:
            if word == v_lower:
                score += _W_VALUE_EXACT
                if str(v) not in matched_values:
                    matched_values.append(str(v))
                match_type = "value"
                hit = True
                break
        if not hit:
            for word in query_words:
                if len(word) >= 3 and word in v_lower:
                    score += _W_VALUE_SUBSTR
                    if str(v) not in matched_values:
                        matched_values.append(str(v))
                    match_type = "value"
                    break

    # Type-aware boost
    if col["type"] == "datetime" and query_words & _DATE_KEYWORDS:
        score += _W_TYPE_BOOST
    elif col["type"] in ("integer", "float") and query_words & _NUM_KEYWORDS:
        score += _W_TYPE_BOOST

    return score, matched_values, match_type


def search_data(
    dataset: str,
    query: str,
    search_scope: str = "all",
    max_results: int = 10,
    storage_path: Optional[str] = None,
) -> dict:
    """Search across column names, values, and metadata.

    Returns column-level results with IDs — tells the agent where to look,
    not the data itself.
    """
    t0 = time.time()
    max_results = min(max(1, max_results), HARD_CAP_SEARCH_MAX_RESULTS)
    store = DataStore(base_path=storage_path or str(get_index_path()))

    idx = store.load(dataset)
    if idx is None:
        return {"error": f"NOT_INDEXED: dataset {dataset!r} is not indexed."}

    query_lower = query.lower().strip()
    query_words = set(query_lower.split())

    scored: list = []
    for col in idx.columns:
        # Apply search_scope filter
        if search_scope == "schema":
            # Only match column names
            name_lower = col["name"].lower()
            if query_lower == name_lower:
                sc = _W_NAME_EXACT
            elif query_lower in name_lower:
                sc = _W_NAME_SUBSTR
            else:
                name_words = set(name_lower.replace("_", " ").split())
                sc = len(query_words & name_words) * _W_NAME_WORD
            if sc > 0:
                scored.append((sc, col, [], "schema"))
        elif search_scope == "values":
            # Only match values
            value_source = []
            if col.get("value_index"):
                value_source = list(col["value_index"].keys())
            elif col.get("top_values"):
                value_source = [tv["value"] for tv in col["top_values"]]
            mv: list = []
            sc = 0
            for v in value_source:
                v_lower = str(v).lower()
                for word in query_words:
                    if word == v_lower:
                        sc += _W_VALUE_EXACT
                        mv.append(str(v))
                        break
                    elif len(word) >= 3 and word in v_lower:
                        sc += _W_VALUE_SUBSTR
                        mv.append(str(v))
                        break
            if sc > 0:
                scored.append((sc, col, mv, "value"))
        else:
            sc, mv, mt = _score_column(col, query_lower, query_words)
            if sc > 0:
                scored.append((sc, col, mv, mt))

    scored.sort(key=lambda x: x[0], reverse=True)

    results = []
    max_score = scored[0][0] if scored else 1
    for sc, col, mv, mt in scored[:max_results]:
        r: dict = {
            "id": f"{dataset}::{col['name']}#column",
            "name": col["name"],
            "type": col["type"],
            "cardinality": col["cardinality"],
            "null_pct": col["null_pct"],
            "match_type": mt,
            "score": round(sc / max_score, 2),
        }
        if mv:
            r["matched_values"] = mv[:10]
        if col.get("ai_summary"):
            r["ai_summary"] = col["ai_summary"]
        results.append(r)

    total_saved = get_total_saved(str(store.base_path))

    return {
        "result": results,
        "_meta": {
            "timing_ms": round((time.time() - t0) * 1000, 1),
            "tokens_saved": 0,
            "total_tokens_saved": total_saved,
        },
    }
