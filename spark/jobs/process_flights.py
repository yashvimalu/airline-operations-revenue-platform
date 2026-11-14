from pathlib import Path

from pyspark.sql import SparkSession

from ingestion.extractors.csv_reader import read_csv
from scripts.config_loader import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main():
    config = load_config()

    source_path = (
        PROJECT_ROOT
        / config["storage"]["raw_path"]
        / "flights.csv"
    )

    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master(config["spark"]["master"])
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel(
            config["spark"]["log_level"]
        )

        flights_df = read_csv(spark, str(source_path))

        print("Flight data schema:")
        flights_df.printSchema()

        print("Total flight records:", flights_df.count())

        flights_df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()