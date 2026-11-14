
import csv
from pathlib import Path


def validate_source_file(file_path, expected_columns=None):
    """Validate a local CSV before Spark ingestion."""
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Source file not found: {path}"
        )

    if path.suffix.lower() != ".csv":
        raise ValueError(
            f"Unsupported file format: {path.suffix}"
        )

    if path.stat().st_size == 0:
        raise ValueError("Source file is empty")

    with path.open(
        "r", newline="", encoding="utf-8-sig"
    ) as file:
        reader = csv.reader(file)
        header = next(reader, None)

    if not header or not any(col.strip() for col in header):
        raise ValueError("CSV header is missing")

    if len(header) != len(set(header)):
        raise ValueError("Duplicate CSV header columns")

    if expected_columns:
        missing = set(expected_columns) - set(header)

        if missing:
            raise ValueError(
                f"Missing required columns: {sorted(missing)}"
            )

    return path
