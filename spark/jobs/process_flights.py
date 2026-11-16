from pathlib import Path
from pyspark.sql import SparkSession
from data_quality.flight_validator import split_valid_invalid, validate_flights
from data_quality.quarantine import build_quarantine, get_quarantine_summary
from ingestion.extractors.csv_reader import read_csv
from scripts.config_loader import load_config
from spark.transformations.flight_deduplication import deduplicate_flights
from spark.transformations.flight_metrics import transform_flights
from spark.transformations.schema_enforcement import apply_schema
from spark.utils.airport_reference import load_airport_reference
from spark.utils.schema_loader import load_contract, load_flight_schema
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]

EXPECTED_COLUMNS = [
    "flight_id",
    "flight_number",
    "airline_code",
    "origin_airport",
    "destination_airport",
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
    "flight_status",
    "aircraft_type",
    "capacity",
    "flight_date",
    "ingestion_timestamp",
]



def main():
    config = load_config()

    source_path = (
        PROJECT_ROOT
        / config["storage"]["raw_path"]
        / "flights.csv"
    )

    spark_config = config["spark"]

    spark = (
        SparkSession.builder
        .appName(spark_config["app_name"])
        .master(spark_config["master"])
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(spark_config.get("shuffle_partitions", 8)),)
        .getOrCreate()
    )
    checkpoint_dir = (
    PROJECT_ROOT / "data" / "checkpoints"
    )

    checkpoint_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # spark.sparkContext.setCheckpointDir(
    #     str(checkpoint_dir)
    # )

    try:
        spark.sparkContext.setLogLevel(
            spark_config.get("log_level", "WARN")
        )

        raw_df = read_csv(
            spark=spark,
            file_path=str(source_path),
            expected_columns=EXPECTED_COLUMNS,
        )

        print("\n=== RAW FLIGHT DATA ===")
        raw_df.printSchema()

        raw_df = raw_df.withColumn(
    "_raw_record",
    F.struct(
        *[
            F.col(column).alias(column)
            for column in raw_df.columns
        ]
    ),
)
        schema = load_flight_schema()

        typed_df = apply_schema(raw_df, schema)

        contract_path = (
            PROJECT_ROOT
            / "docs"
            / "data_contracts"
            / "flight_operations.yaml"
        )

        contract = load_contract(contract_path)

        validated_df = validate_flights(
            typed_df,
            contract,
        )

        valid_df, invalid_df = split_valid_invalid(
            validated_df        )

        valid_df = valid_df.localCheckpoint(eager=True)
        invalid_df = invalid_df.localCheckpoint(eager=True)

        # Deduplicate only records that passed quality checks.
        deduplicated_df, duplicate_df = deduplicate_flights(
            valid_df
        )

        # Combine invalid and duplicate records.
        quarantine_df = build_quarantine(
            invalid_df,
            duplicate_df,
        )

        print("\n=== AIR-19 RECONCILIATION ===")

        raw_count = raw_df.count()
        retained_count = deduplicated_df.count()
        invalid_count = invalid_df.count()
        duplicate_count = duplicate_df.count()
        quarantine_count = quarantine_df.count()

        print(f"Raw records:         {raw_count:,}")
        print(f"Retained records:    {retained_count:,}")
        print(f"Quality rejected:    {invalid_count:,}")
        print(f"Duplicate rejected:  {duplicate_count:,}")
        print(f"Total quarantined:   {quarantine_count:,}")

        assert raw_count == retained_count + quarantine_count
        assert quarantine_count == invalid_count + duplicate_count

        print("\n=== QUARANTINE SUMMARY ===")

        get_quarantine_summary(
            quarantine_df
        ).show(truncate=False)


        print("\n=== DATA QUALITY RESULTS ===")

        validated_df.groupBy("is_valid").count().show()

        print("\n=== VALIDATION ERROR COUNTS ===")

        (
            invalid_df
            .select(
                F.explode("_validation_errors").alias("error_code"))
            .groupBy("error_code")
            .count()
            .orderBy(F.desc("count"))
            .show(truncate=False)
        )

        raw_count = raw_df.count()
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        print(f"Raw records: {raw_count:,}")
        print(f"Valid records: {valid_count:,}")
        print(f"Invalid records: {invalid_count:,}")

        assert raw_count == valid_count + invalid_count


        print("\n=== TYPED FLIGHT DATA ===")
        typed_df.printSchema()

        typed_df.show(5, truncate=False)

        invalid_df = typed_df.filter(
            F.size(F.col("_type_errors")) > 0
        )

        valid_df = typed_df.filter(
            F.size(F.col("_type_errors")) == 0
        )

        transform_config = config.get("transformations", {})

        airport_path = (
            PROJECT_ROOT
            / config["storage"]["airports_reference_path"]
        )

        airports_df = load_airport_reference(
            spark,
            str(airport_path),)

        enriched_df = transform_flights(
            flights=deduplicated_df,
            airports=airports_df,
            threshold_minutes=transform_config.get(
                "on_time_threshold_minutes", 15
            ),
            earth_radius_km=transform_config.get(
                "earth_radius_km", 6371.0088
            ),
        )
        
        print("\n=== ENRICHED FLIGHT DATA ===")

        enriched_df.select(
            "flight_id",
            "origin_airport",
            "destination_airport",
            "departure_delay_minutes",
            "arrival_delay_minutes",
            "scheduled_duration_minutes",
            "actual_duration_minutes",
            "route_distance_km",
            "is_departure_on_time",
            "is_arrival_on_time",
        ).show(10, truncate=False)

        print("\n=== OPERATIONAL SUMMARY ===")

        enriched_df.agg(
            F.count("*").alias("total_flights"),
            F.sum(
                F.when(F.col("is_cancelled"), 1).otherwise(0)
            ).alias("cancelled_flights"),
            F.round(
                F.avg("departure_delay_minutes"), 2
            ).alias("avg_departure_delay_minutes"),
            F.round(
                F.avg("arrival_delay_minutes"), 2
            ).alias("avg_arrival_delay_minutes"),
            F.round(
                F.avg(
                    F.col("is_arrival_on_time").cast("double")
                ) * 100,
                2,
            ).alias("arrival_on_time_pct"),
        ).show(truncate=False)

        raw_count = raw_df.count()
        invalid_count = invalid_df.count()
        valid_count = valid_df.count()

        print(f"Raw records: {raw_count:,}")
        print(f"Type-valid records: {valid_count:,}")
        print(f"Type-invalid records: {invalid_count:,}")

        assert raw_count == valid_count + invalid_count

    finally:
        spark.stop()
if __name__ == "__main__":
    main()