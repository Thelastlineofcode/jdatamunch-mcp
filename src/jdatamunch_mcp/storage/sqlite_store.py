"""SQLite row storage: table creation, batch insert, indexed queries.

Column names are always double-quoted in SQL to support spaces, hyphens, etc.
All user-supplied values are parameterized — no SQL injection surface.
"""

import sqlite3
from pathlib import Path
from typing import Any, Optional

_NULL_VALUES = frozenset([
    "", "null", "NULL", "none", "None", "N/A", "n/a", "NA", "na",
    "NaN", "nan", "-", ".", "#N/A", "#NA", "#NULL!", "n.a.", "N.A.",
])

# SQLite type affinity per inferred column type
_TYPE_AFFINITY = {
    "integer": "INTEGER",
    "float": "REAL",
    "datetime": "TEXT",
    "string": "TEXT",
}

BATCH_SIZE = 10_000
MAX_ROWS_RETURNED = 500


def _qcol(name: str) -> str:
    """Return SQL double-quoted column name with escaped inner quotes."""
    return '"' + name.replace('"', '""') + '"'


def _convert_value(value: str, col_type: str) -> Any:
    """Convert a raw string value to its native Python type for SQLite storage."""
    stripped = value.strip() if value else ""
    if stripped in _NULL_VALUES:
        return None
    if col_type == "integer":
        try:
            return int(stripped)
        except ValueError:
            try:
                return int(float(stripped))
            except ValueError:
                return stripped or None
    elif col_type == "float":
        try:
            return float(stripped)
        except ValueError:
            return stripped or None
    else:
        return stripped if stripped else None


def create_table(
    sqlite_path: Path,
    column_names: list,  # list[str]
    column_types: list,  # list[str] — parallel to column_names
) -> None:
    """Create the rows table (drops if it already exists)."""
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    col_defs = ", ".join(
        f"{_qcol(name)} {_TYPE_AFFINITY.get(ctype, 'TEXT')}"
        for name, ctype in zip(column_names, column_types)
    )
    ddl = f"CREATE TABLE IF NOT EXISTS rows ({col_defs})"

    with sqlite3.connect(str(sqlite_path)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("DROP TABLE IF EXISTS rows")
        conn.execute(ddl)
        conn.commit()


def insert_batch(
    sqlite_path: Path,
    batch: list,            # list of raw string lists
    column_names: list,     # list[str]
    column_types: list,     # list[str]
) -> None:
    """Insert a batch of rows into the rows table."""
    if not batch:
        return

    placeholders = ", ".join("?" * len(column_names))
    col_list = ", ".join(_qcol(n) for n in column_names)
    sql = f"INSERT INTO rows ({col_list}) VALUES ({placeholders})"

    n_cols = len(column_names)

    def _convert_row(row: list) -> tuple:
        return tuple(
            _convert_value(row[i] if i < len(row) else "", column_types[i])
            for i in range(n_cols)
        )

    with sqlite3.connect(str(sqlite_path)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executemany(sql, (_convert_row(r) for r in batch))
        conn.commit()


def create_indexes(
    sqlite_path: Path,
    profiles: list,  # list[ColumnProfile]
    cardinality_threshold: int = 1000,
) -> None:
    """Create SQLite indexes on low-cardinality columns for fast filtering."""
    with sqlite3.connect(str(sqlite_path)) as conn:
        for p in profiles:
            if p.cardinality <= cardinality_threshold and not p.is_unique:
                idx_name = "idx_" + p.name.replace(" ", "_").replace("/", "_")[:50]
                try:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS {_qcol(idx_name)} ON rows ({_qcol(p.name)})"
                    )
                except sqlite3.OperationalError:
                    pass
        conn.commit()


def _build_where(filters: list, schema_columns: list) -> tuple:
    """Build a parameterized WHERE clause from a list of filter objects.

    Returns (where_sql, params).
    Raises ValueError with INVALID_FILTER details on bad input.
    """
    if not filters:
        return "", []

    schema_set = {c["name"] for c in schema_columns}
    clauses = []
    params: list = []

    for f in filters:
        col = f.get("column", "")
        op = f.get("op", "")
        val = f.get("value")

        if col not in schema_set:
            raise ValueError(f"INVALID_COLUMN: {col!r}")

        qc = _qcol(col)

        if op == "eq":
            clauses.append(f"{qc} = ?")
            params.append(val)
        elif op == "neq":
            clauses.append(f"{qc} != ?")
            params.append(val)
        elif op == "gt":
            clauses.append(f"{qc} > ?")
            params.append(val)
        elif op == "gte":
            clauses.append(f"{qc} >= ?")
            params.append(val)
        elif op == "lt":
            clauses.append(f"{qc} < ?")
            params.append(val)
        elif op == "lte":
            clauses.append(f"{qc} <= ?")
            params.append(val)
        elif op == "contains":
            escaped = str(val).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append(f"{qc} LIKE ? ESCAPE '\\'")
            params.append(f"%{escaped}%")
        elif op == "in":
            if not isinstance(val, list) or not val:
                raise ValueError("INVALID_FILTER: 'in' requires a non-empty list value")
            ph = ",".join("?" * len(val))
            clauses.append(f"{qc} IN ({ph})")
            params.extend(val)
        elif op == "is_null":
            if val:
                clauses.append(f"{qc} IS NULL")
            else:
                clauses.append(f"{qc} IS NOT NULL")
        elif op == "between":
            if not isinstance(val, list) or len(val) != 2:
                raise ValueError("INVALID_FILTER: 'between' requires [min, max] list")
            clauses.append(f"{qc} BETWEEN ? AND ?")
            params.extend(val)
        else:
            raise ValueError(f"INVALID_FILTER: unknown operator {op!r}")

    return " AND ".join(clauses), params


def query_rows(
    sqlite_path: Path,
    schema_columns: list,   # list of column dicts from DataIndex
    filters: Optional[list] = None,
    columns: Optional[list] = None,
    order_by: Optional[str] = None,
    order_dir: str = "asc",
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Execute a filtered row query. Returns rows + total_matching count."""
    limit = min(max(1, limit), MAX_ROWS_RETURNED)

    # Column projection
    schema_names = [c["name"] for c in schema_columns]
    if columns:
        select_cols = columns
    else:
        select_cols = schema_names

    col_sql = ", ".join(_qcol(c) for c in select_cols)

    where_sql, params = _build_where(filters or [], schema_columns)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    # Order by
    order_clause = ""
    if order_by and order_by in schema_names:
        direction = "DESC" if order_dir.lower() == "desc" else "ASC"
        order_clause = f"ORDER BY {_qcol(order_by)} {direction}"

    data_sql = (
        f"SELECT rowid, {col_sql} FROM rows {where_clause} "
        f"{order_clause} LIMIT ? OFFSET ?"
    )
    count_sql = f"SELECT COUNT(*) FROM rows {where_clause}"

    with sqlite3.connect(str(sqlite_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=1")

        total = conn.execute(count_sql, params).fetchone()[0]
        cursor = conn.execute(data_sql, params + [limit, offset])

        rows = []
        for row in cursor:
            d = {"_row_id": row["rowid"]}
            for col in select_cols:
                d[col] = row[col]
            rows.append(d)

    return {
        "rows": rows,
        "total_matching": total,
        "returned": len(rows),
        "offset": offset,
        "filters_applied": len(filters) if filters else 0,
        "columns_projected": len(select_cols),
    }


def query_aggregate(
    sqlite_path: Path,
    schema_columns: list,
    group_by: Optional[list] = None,
    aggregations: Optional[list] = None,
    filters: Optional[list] = None,
    order_by: Optional[str] = None,
    order_dir: str = "desc",
    limit: int = 50,
) -> dict:
    """Execute a GROUP BY aggregation query.

    aggregations: list of {"column": ..., "function": ..., "alias": ...}
    """
    if not aggregations:
        raise ValueError("INVALID_FILTER: aggregations is required")

    schema_names = [c["name"] for c in schema_columns]
    VALID_FUNCS = {"count", "sum", "avg", "min", "max", "count_distinct", "median"}

    agg_parts = []
    agg_aliases = []
    for agg in aggregations:
        func = agg.get("function", "").lower()
        col = agg.get("column", "*")
        alias = agg.get("alias", f"{func}_{col}".replace("*", "all").replace(" ", "_"))

        if func not in VALID_FUNCS:
            raise ValueError(f"INVALID_FILTER: unknown aggregation function {func!r}")

        if col != "*" and col not in schema_names:
            raise ValueError(f"INVALID_COLUMN: {col!r}")

        qc = "*" if col == "*" else _qcol(col)

        if func == "count":
            agg_sql = f"COUNT({qc})"
        elif func == "count_distinct":
            agg_sql = f"COUNT(DISTINCT {qc})"
        elif func == "sum":
            agg_sql = f"SUM({qc})"
        elif func == "avg":
            agg_sql = f"AVG({qc})"
        elif func == "min":
            agg_sql = f"MIN({qc})"
        elif func == "max":
            agg_sql = f"MAX({qc})"
        elif func == "median":
            # SQLite has no MEDIAN; approximate with AVG(min+max)/2 or compute in Python
            # For now use AVG as a pragmatic approximation
            agg_sql = f"AVG({qc})"
            alias = alias + "_approx"
        else:
            agg_sql = f"COUNT({qc})"

        agg_parts.append(f"{agg_sql} AS {_qcol(alias)}")
        agg_aliases.append(alias)

    where_sql, params = _build_where(filters or [], schema_columns)
    where_clause = f"WHERE {where_sql}" if where_sql else ""

    group_cols = group_by or []
    for gc in group_cols:
        if gc not in schema_names:
            raise ValueError(f"INVALID_COLUMN: {gc!r} in group_by")

    group_sql = ""
    select_group_cols = ""
    if group_cols:
        group_select = ", ".join(_qcol(c) for c in group_cols)
        group_sql = f"GROUP BY {group_select}"
        select_group_cols = group_select + ", "

    agg_select = ", ".join(agg_parts)
    sql = (
        f"SELECT {select_group_cols}{agg_select} FROM rows "
        f"{where_clause} {group_sql}"
    )

    # ORDER BY
    if order_by:
        direction = "DESC" if order_dir.lower() == "desc" else "ASC"
        if order_by in group_cols:
            ob = _qcol(order_by)
        elif order_by in agg_aliases:
            ob = _qcol(order_by)
        else:
            ob = _qcol(order_by)
        sql += f" ORDER BY {ob} {direction}"

    sql += f" LIMIT ?"

    # Count total groups
    count_sql = (
        f"SELECT COUNT(*) FROM (SELECT 1 FROM rows {where_clause} {group_sql}) AS t"
    )

    with sqlite3.connect(str(sqlite_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=1")

        total_groups = conn.execute(count_sql, params).fetchone()[0]
        cursor = conn.execute(sql, params + [limit])

        groups = []
        for row in cursor:
            d = dict(row)
            groups.append(d)

    return {
        "groups": groups,
        "total_groups": total_groups,
        "returned": len(groups),
    }


def query_sample(
    sqlite_path: Path,
    schema_columns: list,
    n: int = 5,
    method: str = "head",
    columns: Optional[list] = None,
) -> list:
    """Return a sample of rows: head, tail, or random."""
    n = min(max(1, n), 100)
    schema_names = [c["name"] for c in schema_columns]
    select_cols = columns if columns else schema_names

    col_sql = ", ".join(_qcol(c) for c in select_cols)

    if method == "head":
        sql = f"SELECT rowid, {col_sql} FROM rows LIMIT ?"
        params = [n]
    elif method == "tail":
        sql = f"SELECT rowid, {col_sql} FROM rows ORDER BY rowid DESC LIMIT ?"
        params = [n]
    else:  # random
        sql = f"SELECT rowid, {col_sql} FROM rows ORDER BY RANDOM() LIMIT ?"
        params = [n]

    with sqlite3.connect(str(sqlite_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=1")
        cursor = conn.execute(sql, params)

        rows = []
        for row in cursor:
            d = {"_row_id": row["rowid"]}
            for col in select_cols:
                d[col] = row[col]
            rows.append(d)

    return rows
