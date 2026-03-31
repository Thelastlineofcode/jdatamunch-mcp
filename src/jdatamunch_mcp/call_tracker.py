"""Anti-loop detection for jdatamunch-mcp.

Tracks tool calls in a sliding time window and warns when an LLM agent
appears to be paginating through an entire dataset row-by-row, which
defeats the purpose of the tool.
"""

import time
from collections import defaultdict
from typing import Optional

# Sliding window configuration
LOOP_WINDOW_SECONDS = 120
LOOP_MAX_CALLS = 10
PAGINATION_WARN_THRESHOLD = 5  # sequential get_rows offsets before warning

_WARN_MESSAGE = (
    "Sequential pagination detected ({count} calls in {window}s). "
    "Reading all rows defeats jdatamunch-mcp's purpose. "
    "Consider: aggregate() for summaries, search_data() to find values, "
    "describe_column() for distributions."
)

# In-memory state — dicts of {key: [(timestamp, offset), ...]}
_call_log: dict[str, list] = defaultdict(list)


def _prune(key: str, now: float) -> None:
    """Remove entries older than the window."""
    cutoff = now - LOOP_WINDOW_SECONDS
    _call_log[key] = [(ts, off) for ts, off in _call_log[key] if ts >= cutoff]


def record_call(
    tool: str,
    dataset: str,
    offset: int = 0,
) -> Optional[dict]:
    """Record a tool call and return a warning dict if a loop is detected.

    Returns None when everything looks fine, or a warning dict to inject into
    ``_meta.loop_warning`` when a pattern is detected.
    """
    now = time.time()
    key = f"{tool}:{dataset}"
    _prune(key, now)
    _call_log[key].append((now, offset))

    entries = _call_log[key]
    count = len(entries)

    # Hard limit: too many calls in the window
    if count > LOOP_MAX_CALLS:
        return {
            "type": "excessive_calls",
            "message": _WARN_MESSAGE.format(count=count, window=LOOP_WINDOW_SECONDS),
        }

    # Sequential pagination detection (get_rows only)
    if tool == "get_rows" and count >= PAGINATION_WARN_THRESHOLD:
        offsets = [off for _, off in entries]
        # Check if last N offsets are strictly increasing by a consistent step
        recent = offsets[-PAGINATION_WARN_THRESHOLD:]
        diffs = [recent[i + 1] - recent[i] for i in range(len(recent) - 1)]
        if diffs and all(d > 0 for d in diffs) and len(set(diffs)) <= 2:
            return {
                "type": "pagination_loop",
                "message": _WARN_MESSAGE.format(count=count, window=LOOP_WINDOW_SECONDS),
            }

    return None


def reset() -> None:
    """Clear all tracked state (for testing)."""
    _call_log.clear()
