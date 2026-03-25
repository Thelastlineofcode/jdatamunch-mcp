"""Streaming CSV parser with auto-encoding detection."""

import csv
import os
from pathlib import Path
from typing import Generator, Optional

from .types import ColumnInfo, ParsedDataset


def _detect_encoding(path: str) -> str:
    """Detect file encoding using charset-normalizer."""
    try:
        from charset_normalizer import from_path
        result = from_path(path, cp_isolation=["utf-8", "latin-1", "cp1252"])
        if result.best():
            return str(result.best().encoding)
    except Exception:
        pass
    return "utf-8"


def _detect_delimiter(sample: str) -> str:
    """Detect CSV delimiter using csv.Sniffer."""
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
        return dialect.delimiter
    except Exception:
        return ","


def _row_generator(
    path: str,
    encoding: str,
    delimiter: str,
    header_row: int,
) -> Generator:
    """Yield data rows as lists of strings, skipping the header row."""
    with open(path, newline="", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for i, row in enumerate(reader):
            if i == header_row:
                continue  # skip header
            yield row


def parse_csv(
    path: str,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
    header_row: int = 0,
) -> ParsedDataset:
    """Parse a CSV file and return a streaming ParsedDataset.

    The row_iterator is a fresh generator each time this is called.
    Column names come from the header_row.
    """
    path = str(Path(path).resolve())
    file_size = os.path.getsize(path)

    # Detect encoding from file bytes
    if not encoding:
        encoding = _detect_encoding(path)

    # Read a sample for delimiter sniffing
    with open(path, newline="", encoding=encoding, errors="replace") as f:
        sample = f.read(65536)

    # Detect delimiter
    if not delimiter:
        if path.lower().endswith(".tsv"):
            delimiter = "\t"
        else:
            delimiter = _detect_delimiter(sample)

    # Parse header row to get column names
    header: list = []
    with open(path, newline="", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for i, row in enumerate(reader):
            if i == header_row:
                header = row
                break

    columns = [ColumnInfo(name=name.strip(), position=i) for i, name in enumerate(header)]

    # Estimate total row count from file size + sample density
    sample_lines = sample.count("\n")
    if sample_lines > 1:
        bytes_per_row = len(sample.encode(encoding, errors="replace")) / sample_lines
        estimated_rows = max(0, int(file_size / bytes_per_row) - 1)
    else:
        estimated_rows = 0

    metadata = {
        "encoding": encoding,
        "delimiter": delimiter,
        "header_row": header_row,
        "estimated_rows": estimated_rows,
        "file_size": file_size,
    }

    return ParsedDataset(
        columns=columns,
        row_iterator=_row_generator(path, encoding, delimiter, header_row),
        metadata=metadata,
    )
