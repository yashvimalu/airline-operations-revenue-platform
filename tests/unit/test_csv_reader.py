
import pytest
from pyspark.sql import SparkSession

from ingestion.extractors.csv_reader import read_csv


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("FlightIngestionTests")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_csv_reader_loads_records(spark, tmp_path):
    path = tmp_path / "flights.csv"

    path.write_text(
        "flight_id,capacity\n"
        "FLT001,180\n"
        "FLT002,220\n",
        encoding="utf-8",
    )

    df = read_csv(
        spark,
        str(path),
        ["flight_id", "capacity"],
    )

    assert df.count() == 2
    assert df.columns == ["flight_id", "capacity"]


def test_csv_reader_preserves_string_types(spark, tmp_path):
    path = tmp_path / "flights.csv"

    path.write_text(
        "flight_id,capacity\nFLT001,180\n",
        encoding="utf-8",
    )

    df = read_csv(spark, str(path))

    assert df.schema["capacity"].dataType.simpleString() == "string"


def test_csv_reader_rejects_missing_columns(spark, tmp_path):
    path = tmp_path / "flights.csv"

    path.write_text(
        "flight_id\nFLT001\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing required"):
        read_csv(
            spark,
            str(path),
            ["flight_id", "capacity"],
        )
