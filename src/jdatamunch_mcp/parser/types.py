"""Core dataclasses for parsed tabular datasets."""

from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnInfo:
    """Metadata about a single column parsed from the file header."""
    name: str
    position: int


@dataclass
class ParsedDataset:
    """Result of parsing a tabular file. Row iterator is lazy/streaming."""
    columns: list        # list[ColumnInfo]
    row_iterator: Any    # Generator[list[str], None, None] — yields lists of raw strings
    metadata: dict       # encoding, delimiter, header_row, estimated_rows, file_size
