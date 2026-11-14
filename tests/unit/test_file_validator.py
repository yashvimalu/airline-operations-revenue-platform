
import pytest

from ingestion.validators.file_validator import (
    validate_source_file,
)


def test_valid_csv(tmp_path):
    path = tmp_path / "flights.csv"
    path.write_text(
        "flight_id,flight_status\nFLT001,COMPLETED\n",
        encoding="utf-8",
    )

    assert validate_source_file(
        path, ["flight_id", "flight_status"]
    ) == path


def test_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_source_file(tmp_path / "missing.csv")


def test_invalid_extension(tmp_path):
    path = tmp_path / "flights.txt"
    path.write_text("data", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported"):
        validate_source_file(path)


def test_empty_file(tmp_path):
    path = tmp_path / "flights.csv"
    path.touch()

    with pytest.raises(ValueError, match="empty"):
        validate_source_file(path)


def test_missing_columns(tmp_path):
    path = tmp_path / "flights.csv"
    path.write_text(
        "flight_id\nFLT001\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing required"):
        validate_source_file(
            path, ["flight_id", "flight_status"]
        )


def test_duplicate_header(tmp_path):
    path = tmp_path / "flights.csv"
    path.write_text(
        "flight_id,flight_id\nFLT001,FLT002\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate"):
        validate_source_file(path)
