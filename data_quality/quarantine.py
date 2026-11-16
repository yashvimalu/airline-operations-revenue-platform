
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_quarantine(
    invalid_df: DataFrame,
    duplicate_df: DataFrame,
) -> DataFrame:
    """Combine rejected records into a quarantine dataset."""

    rejected = invalid_df.unionByName(
        duplicate_df,
        allowMissingColumns=True,
    )

    rejected = (
        rejected
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                ";",
                F.col("_validation_errors"),
            ),
        )
        .withColumn(
            "quarantine_category",
            F.when(
                F.array_contains(
                    F.col("_validation_errors"),
                    "DUPLICATE_FLIGHT_ID",
                ),
                F.lit("DUPLICATE"),
            ).otherwise(F.lit("DATA_QUALITY")),
        )
    )

    return rejected


def get_quarantine_summary(
    quarantine_df: DataFrame,
) -> DataFrame:
    """Return counts by individual error code."""

    return (
        quarantine_df
        .select(
            F.explode(
                F.col("_validation_errors")
            ).alias("error_code")
        )
        .groupBy("error_code")
        .count()
        .orderBy(F.desc("count"), "error_code")
    )
