"""Parser registry for tabular file formats."""

from pathlib import Path
from typing import Optional

from .types import ParsedDataset
from .csv_parser import parse_csv


def parse_file(
    path: str,
    encoding: Optional[str] = None,
    delimiter: Optional[str] = None,
    header_row: int = 0,
    sheet: Optional[str] = None,
) -> ParsedDataset:
    """Parse a tabular file and return a streaming ParsedDataset."""
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix in (".csv", ".tsv", ".txt"):
        return parse_csv(path, encoding=encoding, delimiter=delimiter, header_row=header_row)
    elif suffix in (".xlsx", ".xls"):
        try:
            from .excel_parser import parse_excel
            return parse_excel(path, sheet=sheet, header_row=header_row)
        except ImportError as e:
            raise ValueError(
                f"Excel support requires openpyxl and xlrd: pip install 'jdatamunch-mcp[excel]' ({e})"
            )
    elif suffix == ".parquet":
        try:
            from .parquet_parser import parse_parquet
            return parse_parquet(path)
        except ImportError as e:
            raise ValueError(
                f"Parquet support requires pyarrow: pip install 'jdatamunch-mcp[parquet]' ({e})"
            )
    elif suffix in (".jsonl", ".ndjson"):
        from .jsonl_parser import parse_jsonl
        return parse_jsonl(path, encoding=encoding)
    else:
        raise ValueError(
            f"Unsupported file format: {suffix!r}. Supported: .csv, .tsv, .xlsx, .xls, .parquet, .jsonl, .ndjson"
        )
