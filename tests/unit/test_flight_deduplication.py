
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.transformations.flight_deduplication import (
    deduplicate_flights,
)

from data_quality.quarantine import (
    build_quarantine,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("FlightDeduplicationTests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session
    session.stop()


def make_valid_df(spark):
    rows = [
        ("FLT001", 180, "2026-04-01 10:00:00"),
        ("FLT001", 200, "2026-04-01 10:05:00"),
        ("FLT002", 150, "2026-04-01 10:10:00"),
    ]

    df = spark.createDataFrame(
        rows,
        ["flight_id", "capacity", "ingestion_timestamp"],
    )

    return (
        df
        .withColumn(
            "ingestion_timestamp",
            F.to_timestamp("ingestion_timestamp"),
        )
        .withColumn(
            "_validation_errors",
            F.expr("cast(array() as array<string>)"),
        )
        .withColumn(
            "is_valid",
            F.lit(True),
        )
    )


def test_latest_record_wins(spark):
    df = make_valid_df(spark)

    retained, duplicates = deduplicate_flights(df)

    assert retained.count() == 2
    assert duplicates.count() == 1

    selected = (
        retained
        .filter(F.col("flight_id") == "FLT001")
        .collect()[0]
    )

    assert selected["capacity"] == 200


def test_duplicate_reason_code(spark):
    df = make_valid_df(spark)

    _, duplicates = deduplicate_flights(df)

    result = duplicates.collect()[0]

    assert "DUPLICATE_FLIGHT_ID" in (
        result["_validation_errors"]
    )

    assert result["is_valid"] is False


def test_reconciliation(spark):
    df = make_valid_df(spark)

    retained, duplicates = deduplicate_flights(df)

    assert df.count() == (
        retained.count() + duplicates.count()
    )


def test_quarantine_contains_duplicates(spark):
    df = make_valid_df(spark)

    _, duplicates = deduplicate_flights(df)

    empty_invalid = spark.createDataFrame(
        [],
        schema=duplicates.schema,
    )

    quarantine = build_quarantine(
        empty_invalid,
        duplicates,
    )

    assert quarantine.count() == 1

    result = quarantine.collect()[0]

    assert result["quarantine_category"] == "DUPLICATE"
    assert "DUPLICATE_FLIGHT_ID" in (
        result["quarantine_reason"]
    )
