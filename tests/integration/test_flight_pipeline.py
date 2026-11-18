
import csv
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F

from ingestion.extractors.csv_reader import read_csv
from spark.utils.schema_loader import (
    load_contract,
    build_spark_schema,
)
from spark.transformations.schema_enforcement import apply_schema
from data_quality.flight_validator import (
    validate_flights,
    split_valid_invalid,
)
from spark.transformations.flight_deduplication import (
    deduplicate_flights,
)
from data_quality.quarantine import build_quarantine
from spark.transformations.flight_metrics import transform_flights
from spark.utils.storage_manager import persist_flight_datasets


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    PROJECT_ROOT / "docs/data_contracts/flight_operations.yaml"
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("AirlineETLIntegrationTests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def pipeline_data(tmp_path):
    contract = load_contract(CONTRACT_PATH)
    columns = [field["name"] for field in contract["fields"]]

    def flight(flight_id, **changes):
        row = {
            "flight_id": flight_id,
            "flight_number": "AI101",
            "airline_code": "AI",
            "origin_airport": "BOM",
            "destination_airport": "DEL",
            "scheduled_departure": "2026-01-15T10:00:00+00:00",
            "actual_departure": "2026-01-15T10:10:00+00:00",
            "scheduled_arrival": "2026-01-15T12:00:00+00:00",
            "actual_arrival": "2026-01-15T12:05:00+00:00",
            "flight_status": "COMPLETED",
            "aircraft_type": "A320",
            "capacity": "180",
            "flight_date": "2026-01-15",
            "ingestion_timestamp": "2026-04-01T10:00:00+00:00",
        }
        row.update(changes)
        return row

    records = [
        flight("FLT001"),
        flight(
            "FLT001",
            capacity="200",
            ingestion_timestamp="2026-04-01T10:05:00+00:00",
        ),
        flight("FLT002", capacity="ABC"),
        flight(
            "FLT003",
            flight_status="CANCELLED",
            actual_departure="",
            actual_arrival="",
        ),
        flight("FLT004", flight_status="UNKNOWN"),
    ]

    csv_path = tmp_path / "flights.csv"

    with csv_path.open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

    return contract, csv_path


def execute_pipeline(spark, contract, csv_path, output_root):
    columns = [field["name"] for field in contract["fields"]]

    raw_df = read_csv(
        spark,
        str(csv_path),
        expected_columns=columns,
    )

    raw_df = raw_df.withColumn(
        "_raw_record",
        F.struct(
            *[F.col(c).alias(c) for c in columns]
        ),
    )

    schema = build_spark_schema(contract)
    typed_df = apply_schema(raw_df, schema)

    validated_df = validate_flights(typed_df, contract)
    valid_df, invalid_df = split_valid_invalid(validated_df)

    retained_df, duplicate_df = deduplicate_flights(valid_df)

    quarantine_df = build_quarantine(
        invalid_df, duplicate_df
    )

    airports_df = spark.createDataFrame(
        [
            ("BOM", 19.0896, 72.8656),
            ("DEL", 28.5562, 77.1000),
        ],
        ["iata_code", "latitude", "longitude"],
    )

    enriched_df = transform_flights(
        flights=retained_df,
        airports=airports_df,
    )

    config = {
        "storage": {
            "processed_path": str(output_root / "curated"),
            "quarantine_path": str(output_root / "quarantine"),
        },
        "output": {
            "write_mode": "overwrite",
            "curated_dataset": "flights",
            "quarantine_dataset": "flights",
        },
    }

    paths = persist_flight_datasets(
        spark=spark,
        curated_df=enriched_df,
        quarantine_df=quarantine_df,
        config=config,
    )

    return (
        raw_df,
        spark.read.parquet(paths.curated),
        spark.read.parquet(paths.quarantine),
    )


def test_end_to_end_reconciliation(
    spark, pipeline_data, tmp_path
):
    contract, csv_path = pipeline_data

    raw, curated, quarantine = execute_pipeline(
        spark, contract, csv_path, tmp_path / "output"
    )

    assert raw.count() == 5
    assert curated.count() == 2
    assert quarantine.count() == 3
    assert raw.count() == curated.count() + quarantine.count()

    assert curated.select("flight_id").distinct().count() == 2

    assert set(
        row.flight_id
        for row in curated.select("flight_id").collect()
    ) == {"FLT001", "FLT003"}

    selected = curated.filter(
        F.col("flight_id") == "FLT001"
    ).first()

    assert selected["capacity"] == 200
    assert selected["departure_delay_minutes"] == 10

    rejected = {
        row["flight_id"]: row
        for row in quarantine.collect()
    }

    assert "DUPLICATE_FLIGHT_ID" in (
        rejected["FLT001"]["_validation_errors"]
    )
    assert "INVALID_TYPE:capacity" in (
        rejected["FLT002"]["_validation_errors"]
    )
    assert "INVALID_VALUE:flight_status" in (
        rejected["FLT004"]["_validation_errors"]
    )

    assert rejected["FLT002"]["_raw_record"]["capacity"] == "ABC"

    assert quarantine.filter(
        F.size("_validation_errors") == 0
    ).count() == 0


def test_rerun_does_not_accumulate(
    spark, pipeline_data, tmp_path
):
    contract, csv_path = pipeline_data
    output_root = tmp_path / "rerun_output"

    execute_pipeline(spark, contract, csv_path, output_root)

    _, curated, quarantine = execute_pipeline(
        spark, contract, csv_path, output_root
    )

    assert curated.count() == 2
    assert quarantine.count() == 3
