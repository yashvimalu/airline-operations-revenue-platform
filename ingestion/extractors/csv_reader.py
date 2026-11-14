from pyspark.sql import SparkSession, DataFrame

from ingestion.validators.file_validator import (
    validate_source_file,
)


def read_csv(
    spark: SparkSession,
    file_path: str,
    expected_columns=None,
) -> DataFrame:
    """Read a validated local CSV into a Spark DataFrame."""

    validated_path = validate_source_file(
        file_path,
        expected_columns=expected_columns,
    )

    df = (
        spark.read
        .option("header", "true")
        #we're deliberately using inferSchema=false.Schema inference can be expensive and inconsistent. 
        # Our next story will use the AIR-10 data contract to define an explicit Spark schema.
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .csv(str(validated_path))
    )

    if expected_columns:
        missing = set(expected_columns) - set(df.columns)

        if missing:
            raise ValueError(
                f"Spark DataFrame missing columns: {missing}"
            )

    return df