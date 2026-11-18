
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


PARTITION_COLUMNS = ["flight_year", "flight_month"]


def add_flight_partitions(df: DataFrame) -> DataFrame:
    """Derive partition columns from the flight date."""

    if "flight_date" not in df.columns:
        raise ValueError("Missing flight_date column")

    return (
        df
        .withColumn(
            "flight_year",
            F.year(F.col("flight_date")),
        )
        .withColumn(
            "flight_month",
            F.month(F.col("flight_date")),
        )
    )


def write_partitioned_parquet(
    df: DataFrame,
    output_path: str,
    write_mode: str = "overwrite",
) -> None:
    """Write a flight dataset as partitioned Parquet."""

    if not output_path or not output_path.strip():
        raise ValueError("Output path cannot be empty")

    if write_mode != "overwrite":
        raise ValueError(
            "AIR-20 supports overwrite mode only"
        )

    partitioned_df = add_flight_partitions(df)

    # Null partition values are allowed for quarantine:
    # Spark stores them under its default null partition name.
    (
        partitioned_df.write
        .mode(write_mode)
        .partitionBy(*PARTITION_COLUMNS)
        .parquet(output_path)
    )


def read_partitioned_parquet(
    spark,
    output_path: str,
) -> DataFrame:
    """Read the persisted dataset for verification."""

    return spark.read.parquet(output_path)
