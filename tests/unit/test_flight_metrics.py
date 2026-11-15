
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.transformations.flight_metrics import (
    transform_flights,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("FlightMetricsTests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture
def airports(spark):
    return spark.createDataFrame(
        [
            ("BOM", 19.0896, 72.8656),
            ("DEL", 28.5562, 77.1000),
        ],
        ["iata_code", "latitude", "longitude"],
    )


def make_flights(spark, rows):
    df = spark.createDataFrame(
        rows,
        """
        flight_id string,
        origin_airport string,
        destination_airport string,
        flight_status string,
        scheduled_departure string,
        actual_departure string,
        scheduled_arrival string,
        actual_arrival string
        """,
    )

    for column in [
        "scheduled_departure",
        "actual_departure",
        "scheduled_arrival",
        "actual_arrival",
    ]:
        df = df.withColumn(
            column,
            F.to_timestamp(column),
        )

    return df


def test_completed_flight_metrics(spark, airports):
    flights = make_flights(
        spark,
        [
            (
                "FLT001",
                "BOM",
                "DEL",
                "COMPLETED",
                "2026-01-01 10:00:00",
                "2026-01-01 10:25:00",
                "2026-01-01 12:00:00",
                "2026-01-01 12:10:00",
            ),
        ],
    )

    result = transform_flights(
        flights,
        airports,
    ).collect()[0]

    assert result["departure_delay_minutes"] == 25
    assert result["arrival_delay_minutes"] == 10
    assert result["scheduled_duration_minutes"] == 120
    assert result["actual_duration_minutes"] == 105

    assert result["is_departure_on_time"] is False
    assert result["is_arrival_on_time"] is True

    assert 1100 < result["route_distance_km"] < 1200


def test_cancelled_flight_metrics(spark, airports):
    flights = make_flights(
        spark,
        [
            (
                "FLT002",
                "BOM",
                "DEL",
                "CANCELLED",
                "2026-01-01 10:00:00",
                None,
                "2026-01-01 12:00:00",
                None,
            ),
        ],
    )

    result = transform_flights(
        flights,
        airports,
    ).collect()[0]

    assert result["is_cancelled"] is True
    assert result["departure_delay_minutes"] is None
    assert result["arrival_delay_minutes"] is None
    assert result["actual_duration_minutes"] is None
    assert result["is_departure_on_time"] is None
    assert result["is_arrival_on_time"] is None


def test_early_departure(spark, airports):
    flights = make_flights(
        spark,
        [
            (
                "FLT003",
                "BOM",
                "DEL",
                "COMPLETED",
                "2026-01-01 10:00:00",
                "2026-01-01 09:50:00",
                "2026-01-01 12:00:00",
                "2026-01-01 11:55:00",
            ),
        ],
    )

    result = transform_flights(
        flights,
        airports,
    ).collect()[0]

    assert result["departure_delay_minutes"] == -10
    assert result["arrival_delay_minutes"] == -5
    assert result["is_departure_on_time"] is True
    assert result["is_arrival_on_time"] is True


def test_unknown_airport_distance(spark, airports):
    flights = make_flights(
        spark,
        [
            (
                "FLT004",
                "XXX",
                "DEL",
                "COMPLETED",
                "2026-01-01 10:00:00",
                "2026-01-01 10:00:00",
                "2026-01-01 12:00:00",
                "2026-01-01 12:00:00",
            ),
        ],
    )

    result = transform_flights(
        flights,
        airports,
    ).collect()[0]

    assert result["route_distance_km"] is None
