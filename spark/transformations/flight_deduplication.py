
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate_flights(df: DataFrame):
    """
    Deduplicate valid flights using a deterministic policy.

    Returns:
        retained_df: One record per flight_id.
        duplicate_df: Rejected duplicate records.
    """

    required = {
        "flight_id",
        "ingestion_timestamp",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing deduplication columns: {sorted(missing)}"
        )

    # AIR-17 must have validated flight_id first.
    if "is_valid" not in df.columns:
        raise ValueError(
            "Deduplication requires an is_valid column"
        )

    # Use the actual data fields, excluding internal metadata.
    business_columns = sorted(
        column
        for column in df.columns
        if not column.startswith("_")
        and column != "is_valid"
    )

    # Canonical representation of each row for tie-breaking.
    row_hash = F.sha2(
        F.to_json(
            F.struct(
                *[
                    F.col(column).alias(column)
                    for column in business_columns
                ]
            ),
            options={"ignoreNullFields": "false"},
        ),
        256,
    )

    ranked = df.withColumn(
        "_row_hash",
        row_hash,
    )

    window = (
        Window
        .partitionBy("flight_id")
        .orderBy(
            F.col("ingestion_timestamp").desc_nulls_last(),
            F.col("_row_hash").asc(),
        )
    )

    ranked = ranked.withColumn(
        "_dedup_rank",
        F.row_number().over(window),
    )

    retained_df = (
        ranked
        .filter(F.col("_dedup_rank") == 1)
        .drop("_row_hash", "_dedup_rank")
    )

    duplicate_df = (
        ranked
        .filter(F.col("_dedup_rank") > 1)
        .withColumn(
            "_validation_errors",
            F.concat(
                F.col("_validation_errors"),
                F.array(F.lit("DUPLICATE_FLIGHT_ID")),
            ),
        )
        .withColumn("is_valid", F.lit(False))
        .drop("_row_hash", "_dedup_rank")
    )

    return retained_df, duplicate_df
