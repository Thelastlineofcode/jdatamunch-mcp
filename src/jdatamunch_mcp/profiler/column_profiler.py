"""Single-pass streaming column profiler.

Processes rows one at a time using per-column accumulators.
Designed to work with index_local.py's main loop where profiling
and SQLite loading happen in the same pass over the data.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Optional

_NULL_VALUES = frozenset([
    "", "null", "NULL", "none", "None", "N/A", "n/a", "NA", "na",
    "NaN", "nan", "-", ".", "#N/A", "#NA", "#NULL!", "n.a.", "N.A.",
])

# Type rank: lower = more specific
_TYPE_RANK = {"integer": 0, "float": 1, "datetime": 2, "string": 3}
_TYPE_FROM_RANK = {0: "integer", 1: "float", 2: "datetime", 3: "string"}

MAX_CARDINALITY_TRACK = 5_000   # stop adding new keys to value_counts after this
SAMPLE_SIZE = 10                 # distinct non-null samples to collect
RESERVOIR_SIZE = 10_000          # numeric values for approximate median

VALUE_INDEX_CARDINALITY_LIMIT = 1_000  # full value map stored if cardinality <= this
TOP_VALUES_LIMIT = 50                   # top values stored for high-cardinality columns

# Common datetime patterns (regex → strptime format string)
_DATETIME_PATTERNS = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "%Y-%m-%d"),
    (re.compile(r"^\d{2}/\d{2}/\d{4}$"), "%m/%d/%Y"),
    (re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"), "%Y-%m-%dT%H:%M:%S"),
    (re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"), "%Y-%m-%d %H:%M:%S"),
    (re.compile(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}"), "%m/%d/%Y %H:%M:%S"),
    # US date + 12h time with AM/PM (e.g. "01/15/2020 12:00:00 AM")
    (re.compile(r"^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [AP]M$"), "%m/%d/%Y %I:%M:%S %p"),
]


def _is_datetime_str(value: str) -> bool:
    for rx, _ in _DATETIME_PATTERNS:
        if rx.match(value):
            return True
    return False


def _get_datetime_format(value: str) -> Optional[str]:
    for rx, fmt in _DATETIME_PATTERNS:
        if rx.match(value):
            return fmt
    return None


@dataclass
class _ColAcc:
    """Per-column accumulator updated once per row."""
    name: str
    position: int
    # Type tracking (rank only advances upward)
    type_rank: int = 0
    # Row counts
    count: int = 0        # non-null rows
    null_count: int = 0
    # Numeric stats (valid when type_rank <= 1)
    num_min: float = field(default_factory=lambda: float("inf"))
    num_max: float = field(default_factory=lambda: float("-inf"))
    num_sum: float = 0.0
    # Reservoir for approximate median (first RESERVOIR_SIZE numeric values)
    reservoir: list = field(default_factory=list)
    # Cardinality / value frequency
    value_counts: dict = field(default_factory=dict)
    cardinality_overflow: bool = False
    # Samples: first SAMPLE_SIZE distinct non-null values seen
    samples: list = field(default_factory=list)
    _samples_set: set = field(default_factory=set)
    # Datetime range (valid when type_rank == 2)
    dt_min: Optional[str] = None
    dt_max: Optional[str] = None
    dt_format: Optional[str] = None


def update_acc(acc: _ColAcc, raw_value: str) -> None:
    """Update accumulator with one raw string value from the CSV."""
    stripped = raw_value.strip() if raw_value else ""

    if stripped in _NULL_VALUES:
        acc.null_count += 1
        return

    acc.count += 1

    # --- Type detection & promotion ---
    if acc.type_rank == 0:  # currently integer
        try:
            int(stripped)
        except ValueError:
            acc.type_rank = 1  # promote to float

    if acc.type_rank == 1:  # currently float
        try:
            float(stripped)
        except ValueError:
            # Check datetime before falling to string
            if _is_datetime_str(stripped):
                acc.type_rank = 2
            else:
                acc.type_rank = 3  # string

    if acc.type_rank == 2:  # currently datetime
        if not _is_datetime_str(stripped):
            acc.type_rank = 3  # string
        elif acc.dt_format is None:
            acc.dt_format = _get_datetime_format(stripped)

    # --- Numeric stats ---
    if acc.type_rank <= 1:
        try:
            num = float(stripped)
            if num < acc.num_min:
                acc.num_min = num
            if num > acc.num_max:
                acc.num_max = num
            acc.num_sum += num
            if len(acc.reservoir) < RESERVOIR_SIZE:
                acc.reservoir.append(num)
        except ValueError:
            pass

    # --- Datetime min/max ---
    if acc.type_rank == 2:
        if acc.dt_min is None or stripped < acc.dt_min:
            acc.dt_min = stripped
        if acc.dt_max is None or stripped > acc.dt_max:
            acc.dt_max = stripped

    # --- Cardinality / value counts ---
    if stripped in acc.value_counts:
        acc.value_counts[stripped] += 1
    elif not acc.cardinality_overflow:
        if len(acc.value_counts) < MAX_CARDINALITY_TRACK:
            acc.value_counts[stripped] = 1
        else:
            acc.cardinality_overflow = True
            acc.value_counts[stripped] = 1

    # --- Samples ---
    if len(acc.samples) < SAMPLE_SIZE and stripped not in acc._samples_set:
        acc.samples.append(stripped)
        acc._samples_set.add(stripped)


def _compute_median(reservoir: list) -> Optional[float]:
    if not reservoir:
        return None
    sorted_vals = sorted(reservoir)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0
    return float(sorted_vals[mid])


@dataclass
class ColumnProfile:
    """Fully computed profile for a single column."""
    name: str
    position: int
    type: str              # "integer", "float", "datetime", "string"
    count: int             # non-null row count
    null_count: int
    null_pct: float
    cardinality: int
    cardinality_is_exact: bool
    is_unique: bool
    is_primary_key_candidate: bool
    min: Optional[Any]
    max: Optional[Any]
    mean: Optional[float]
    median: Optional[float]
    sample_values: list
    value_index: Optional[dict]   # full {value: count} for cardinality <= 1000
    top_values: Optional[list]    # [{"value": ..., "count": ...}] for high-cardinality
    datetime_min: Optional[str] = None
    datetime_max: Optional[str] = None
    datetime_format: Optional[str] = None
    ai_summary: Optional[str] = None


def finalize_profile(acc: _ColAcc) -> ColumnProfile:
    """Build a ColumnProfile from a completed _ColAcc."""
    total = acc.count + acc.null_count
    null_pct = round(acc.null_count / total * 100, 1) if total > 0 else 0.0
    col_type = _TYPE_FROM_RANK[acc.type_rank]

    cardinality = len(acc.value_counts)
    cardinality_is_exact = not acc.cardinality_overflow

    is_unique = (cardinality_is_exact and cardinality == acc.count and acc.count > 0)
    is_pk_candidate = (
        is_unique
        and acc.null_count == 0
        and col_type in ("integer", "string")
    )

    # Numeric stats
    if col_type in ("integer", "float") and acc.count > 0:
        raw_min = acc.num_min
        raw_max = acc.num_max
        mean_val = round(acc.num_sum / acc.count, 4)
        median_val = _compute_median(acc.reservoir)
        if col_type == "integer":
            min_val = int(raw_min) if raw_min != float("inf") else None
            max_val = int(raw_max) if raw_max != float("-inf") else None
            median_val = round(median_val, 1) if median_val is not None else None
        else:
            min_val = raw_min if raw_min != float("inf") else None
            max_val = raw_max if raw_max != float("-inf") else None
    else:
        min_val = max_val = mean_val = median_val = None

    # Convert sample values to their native type
    samples: list = []
    for s in acc.samples:
        if col_type == "integer":
            try:
                samples.append(int(s))
                continue
            except ValueError:
                pass
        elif col_type == "float":
            try:
                samples.append(float(s))
                continue
            except ValueError:
                pass
        samples.append(s)

    # Value index / top values
    if cardinality <= VALUE_INDEX_CARDINALITY_LIMIT:
        value_index: Optional[dict] = {}
        for val_str, cnt in acc.value_counts.items():
            if col_type == "integer":
                try:
                    key: Any = int(val_str)
                except ValueError:
                    key = val_str
            elif col_type == "float":
                try:
                    key = float(val_str)
                except ValueError:
                    key = val_str
            else:
                key = val_str
            value_index[str(key)] = cnt
        top_values = None
    else:
        value_index = None
        sorted_vals = sorted(acc.value_counts.items(), key=lambda x: x[1], reverse=True)
        top_values = [{"value": v, "count": c} for v, c in sorted_vals[:TOP_VALUES_LIMIT]]

    return ColumnProfile(
        name=acc.name,
        position=acc.position,
        type=col_type,
        count=acc.count,
        null_count=acc.null_count,
        null_pct=null_pct,
        cardinality=cardinality,
        cardinality_is_exact=cardinality_is_exact,
        is_unique=is_unique,
        is_primary_key_candidate=is_pk_candidate,
        min=min_val,
        max=max_val,
        mean=mean_val,
        median=median_val,
        sample_values=samples,
        value_index=value_index,
        top_values=top_values,
        datetime_min=acc.dt_min if col_type == "datetime" else None,
        datetime_max=acc.dt_max if col_type == "datetime" else None,
        datetime_format=acc.dt_format if col_type == "datetime" else None,
    )


def infer_types_from_sample(
    accs: list,  # list[_ColAcc]
    sample_rows: list,
) -> list:
    """Run a subset of rows through the accumulators to detect preliminary types.

    Used to determine column types before creating the SQLite schema.
    Returns the modified accs list (same objects, updated in-place).
    """
    n_cols = len(accs)
    for row in sample_rows:
        for i, acc in enumerate(accs):
            raw = row[i] if i < len(row) else ""
            update_acc(acc, raw)
    return accs
