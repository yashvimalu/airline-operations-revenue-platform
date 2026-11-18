
import pytest

from pyspark.sql import SparkSession

from spark.utils.parquet_writer import (
    add_flight_partitions,
    write_partitioned_parquet,
    read_partitioned_parquet,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("ParquetWriterTests")
        .getOrCreate()
    )

    yield session
    session.stop()


def test_partition_columns(spark):
    df = spark.createDataFrame(
        [
            ("FLT001", "2026-01-15"),
            ("FLT002", "2026-03-20"),
        ],
        ["flight_id", "flight_date"],
    )

    from pyspark.sql import functions as F

    df = df.withColumn(
        "flight_date",
        F.to_date("flight_date"),
    )

    result = add_flight_partitions(df).collect()

    assert result[0]["flight_year"] == 2026
    assert result[0]["flight_month"] == 1
    assert result[1]["flight_month"] == 3


def test_parquet_write_and_read(spark, tmp_path):
    df = spark.createDataFrame(
        [("FLT001", "2026-01-15")],
        ["flight_id", "flight_date"],
    )

    from pyspark.sql import functions as F

    df = df.withColumn(
        "flight_date",
        F.to_date("flight_date"),
    )

    output_path = str(tmp_path / "flights")

    write_partitioned_parquet(
        df,
        output_path,
    )

    saved = read_partitioned_parquet(
        spark,
        output_path,
    )

    assert saved.count() == 1
    assert "flight_year" in saved.columns
    assert "flight_month" in saved.columns


def test_overwrite_does_not_accumulate_rows(
    spark,
    tmp_path,
):
    df = spark.createDataFrame(
        [("FLT001", "2026-01-15")],
        ["flight_id", "flight_date"],
    )

    from pyspark.sql import functions as F

    df = df.withColumn(
        "flight_date",
        F.to_date("flight_date"),
    )

    output_path = str(tmp_path / "flights")

    write_partitioned_parquet(df, output_path)
    write_partitioned_parquet(df, output_path)

    saved = read_partitioned_parquet(
        spark,
        output_path,
    )

    assert saved.count() == 1
    