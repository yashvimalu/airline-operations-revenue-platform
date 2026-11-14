from pyspark.sql import DataFrame, SparkSession

from ingestion.validators.file_validator import validate_source_file


def read_csv(
    spark: SparkSession,
    file_path: str,
) -> DataFrame:
    validated_path = validate_source_file(file_path)

    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("inferSchema", "false")
        #we're deliberately using inferSchema=false. Schema inference can be expensive and inconsistent. 
        # Our next story will use the AIR-10 data contract to define an explicit Spark schema.
        .csv(str(validated_path))
    )