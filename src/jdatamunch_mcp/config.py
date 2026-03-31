"""Environment variable handling and defaults for jdatamunch-mcp."""

import os
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Hard caps for tool parameters (prevent accidental token budget explosions)
# ---------------------------------------------------------------------------
HARD_CAP_AGGREGATE_LIMIT = 1000
HARD_CAP_DESCRIBE_COLUMN_TOP_N = 200
HARD_CAP_DESCRIBE_COLUMN_BINS = 50
HARD_CAP_SEARCH_MAX_RESULTS = 50

# Wide-table protections
MAX_COLUMNS_DESCRIBE = 60   # max column profiles per describe_dataset response
MAX_COLUMNS_ROWS = 30       # max auto-projected columns for get_rows/sample_rows

# Response-level token budget
DEFAULT_MAX_RESPONSE_TOKENS = 8_000    # ~32 KB JSON
ABSOLUTE_MAX_RESPONSE_TOKENS = 16_000  # hard ceiling


def get_max_response_tokens() -> int:
    return min(
        int(os.environ.get("JDATAMUNCH_MAX_RESPONSE_TOKENS", str(DEFAULT_MAX_RESPONSE_TOKENS))),
        ABSOLUTE_MAX_RESPONSE_TOKENS,
    )


def get_index_path(override: Optional[str] = None) -> Path:
    """Return the base index storage path."""
    if override:
        return Path(override)
    return Path(os.environ.get("DATA_INDEX_PATH", str(Path.home() / ".data-index")))


def get_max_rows() -> int:
    return int(os.environ.get("JDATAMUNCH_MAX_ROWS", "5000000"))


def get_share_savings() -> bool:
    return os.environ.get("JDATAMUNCH_SHARE_SAVINGS", "1") != "0"


def get_use_ai_summaries() -> bool:
    v = os.environ.get("JDATAMUNCH_USE_AI_SUMMARIES", "true").lower()
    return v not in ("false", "0", "no", "off")
