
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_airport_reference(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    """Read and validate airport reference coordinates."""

    airports = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(file_path)
    )

    required = {
        "iata_code",
        "latitude_deg",
        "longitude_deg",
    }

    missing = required - set(airports.columns)

    if missing:
        raise ValueError(
            f"Airport reference missing columns: {sorted(missing)}"
        )

    airports = airports.select(
        F.upper(F.trim("iata_code")).alias("iata_code"),
        F.col("latitude_deg").cast("double").alias("latitude"),
        F.col("longitude_deg").cast("double").alias("longitude"),
    )

    invalid = airports.filter(
        F.col("iata_code").isNull()
        | ~F.col("iata_code").rlike("^[A-Z]{3}$")
        | F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | ~F.col("latitude").between(-90, 90)
        | ~F.col("longitude").between(-180, 180)
    )

    if invalid.limit(1).count() > 0:
        raise ValueError(
            "Airport reference contains invalid codes or coordinates"
        )

    duplicates = (
        airports.groupBy("iata_code")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            "Airport reference contains duplicate IATA codes"
        )

    return airports
