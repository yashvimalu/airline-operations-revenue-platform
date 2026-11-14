from pathlib import Path
from pyspark.sql import SparkSession
from ingestion.extractors.csv_reader import read_csv
from scripts.config_loader import load_config
from spark.transformations.schema_enforcement import apply_schema
from spark.utils.schema_loader import load_flight_schema
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
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_config.get("shuffle_partitions", 8)),
        )
        .getOrCreate()
    )

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

        schema = load_flight_schema()

        typed_df = apply_schema(raw_df, schema)

        print("\n=== TYPED FLIGHT DATA ===")
        typed_df.printSchema()

        typed_df.show(5, truncate=False)

        invalid_df = typed_df.filter(
            F.size(F.col("_type_errors")) > 0
        )

        valid_df = typed_df.filter(
            F.size(F.col("_type_errors")) == 0
        )

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