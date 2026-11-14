from pathlib import Path

from pyspark.sql import SparkSession

from ingestion.extractors.csv_reader import read_csv
from scripts.config_loader import load_config


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

        flights_df = read_csv(
            spark=spark,
            file_path=str(source_path),
            expected_columns=EXPECTED_COLUMNS,
        )

        print("\n=== FLIGHT INGESTION ===")

        flights_df.printSchema()

        record_count = flights_df.count()

        print(f"Total records ingested: {record_count:,}")

        if record_count == 0:
            raise ValueError("Flight dataset has no records")

        flights_df.show(5, truncate=False)

        print("Flight ingestion completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
