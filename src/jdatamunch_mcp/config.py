"""Environment variable handling and defaults for jdatamunch-mcp."""

import os
from pathlib import Path
from typing import Optional


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
