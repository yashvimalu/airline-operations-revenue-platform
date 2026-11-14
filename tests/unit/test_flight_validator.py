
import pytest

from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from data_quality.flight_validator import (
    validate_flights,
    split_valid_invalid,
)

from spark.utils.schema_loader import (
    load_contract,
    PROJECT_ROOT,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("FlightQualityTests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture
def contract():
    return load_contract(
        PROJECT_ROOT
        / "docs"
        / "data_contracts"
        / "flight_operations.yaml"
    )


def make_flight(**overrides):
    flight = {
        "flight_id": "FLT001",
        "flight_number": "AI101",
        "airline_code": "AI",
        "origin_airport": "BOM",
        "destination_airport": "DEL",
        "scheduled_departure": "2026-01-01 10:00:00",
        "actual_departure": "2026-01-01 10:05:00",
        "scheduled_arrival": "2026-01-01 12:00:00",
        "actual_arrival": "2026-01-01 12:10:00",
        "flight_status": "COMPLETED",
        "aircraft_type": "A320",
        "capacity": 180,
        "flight_date": "2026-01-01",
        "ingestion_timestamp": "2026-04-01 00:00:00",
    }

    flight.update(overrides)

    return flight



def validate_one(spark, contract, **overrides):
    flight = make_flight(**overrides)

    # Raw input schema: timestamps are strings initially.
    raw_schema = StructType([
        StructField("flight_id", StringType(), True),
        StructField("flight_number", StringType(), True),
        StructField("airline_code", StringType(), True),
        StructField("origin_airport", StringType(), True),
        StructField("destination_airport", StringType(), True),
        StructField("scheduled_departure", StringType(), True),
        StructField("actual_departure", StringType(), True),
        StructField("scheduled_arrival", StringType(), True),
        StructField("actual_arrival", StringType(), True),
        StructField("flight_status", StringType(), True),
        StructField("aircraft_type", StringType(), True),
        StructField("capacity", IntegerType(), True),
        StructField("flight_date", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ])

    df = spark.createDataFrame(
        [flight],
        schema=raw_schema,
    )

    for column in [
        "scheduled_departure",
        "actual_departure",
        "scheduled_arrival",
        "actual_arrival",
        "ingestion_timestamp",
    ]:
        df = df.withColumn(
            column,
            F.to_timestamp(F.col(column)),
        )

    df = df.withColumn(
        "flight_date",
        F.to_date(F.col("flight_date")),
    )

    result = validate_flights(
        df,
        contract,
    ).collect()[0]

    return result["_validation_errors"]

def test_valid_flight(spark, contract):
    assert validate_one(spark, contract) == []


def test_same_airport(spark, contract):
    errors = validate_one(
        spark,
        contract,
        destination_airport="BOM",
    )

    assert "SAME_ORIGIN_DESTINATION" in errors


def test_negative_capacity(spark, contract):
    errors = validate_one(
        spark,
        contract,
        capacity=-50,
    )

    assert "MIN_EXCLUSIVE:capacity" in errors


def test_invalid_airport_code(spark, contract):
    errors = validate_one(
        spark,
        contract,
        origin_airport="MUMBAI",
    )

    assert "INVALID_FORMAT:origin_airport" in errors


def test_invalid_status(spark, contract):
    errors = validate_one(
        spark,
        contract,
        flight_status="UNKNOWN",
    )

    assert "INVALID_VALUE:flight_status" in errors


def test_cancelled_flight_with_actual_times(spark, contract):
    errors = validate_one(
        spark,
        contract,
        flight_status="CANCELLED",
    )

    assert "CANCELLED_HAS_ACTUAL_TIMES" in errors


def test_completed_flight_missing_actual_arrival(spark, contract):
    errors = validate_one(
        spark,
        contract,
        actual_arrival=None,
    )

    assert "COMPLETED_MISSING_ACTUAL_TIMES" in errors


def test_invalid_schedule_order(spark, contract):
    errors = validate_one(
        spark,
        contract,
        scheduled_arrival="2026-01-01 09:00:00",
    )

    assert "INVALID_SCHEDULED_TIME_ORDER" in errors
