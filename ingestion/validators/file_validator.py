from pathlib import Path


def validate_source_file(file_path: str | Path) -> Path:
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Source file does not exist: {path}"
        )

    if path.suffix.lower() != ".csv":
        raise ValueError(
            f"Expected CSV file, received: {path.suffix}"
        )

    if path.stat().st_size == 0:
        raise ValueError(
            f"Source file is empty: {path}"
        )

    return path