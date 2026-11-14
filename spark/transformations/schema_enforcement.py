
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    TimestampType,
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
    StringType,
)


def apply_schema(raw_df: DataFrame, schema):
    """
    Convert raw string columns to contract-defined types.

    Returns a DataFrame with:
      - typed contract columns
      - _type_errors: array of conversion error codes
    """

    missing = set(schema.fieldNames()) - set(raw_df.columns)

    if missing:
        raise ValueError(
            f"Missing required input columns: {sorted(missing)}"
        )

    # Our generator emits ISO 8601 UTC timestamps.
    # The Spark session should use UTC.
    converted_columns = []
    error_columns = []

    for field in schema.fields:
        name = field.name
        dtype = field.dataType

        raw = F.col(name)
        cleaned = F.trim(raw)

        if isinstance(dtype, TimestampType):
            converted = F.to_timestamp(cleaned)

        elif isinstance(dtype, DateType):
            converted = F.to_date(cleaned, "yyyy-MM-dd")

        elif isinstance(
            dtype,
            (IntegerType, LongType, DoubleType, DecimalType),
        ):
            converted = cleaned.cast(dtype)

        elif isinstance(dtype, StringType):
            converted = raw

        else:
            raise ValueError(
                f"Unsupported conversion for {name}: {dtype}"
            )

        converted_columns.append(
            converted.alias(name)
        )

        conversion_failed = (
            raw.isNotNull()
            & (cleaned != "")
            & converted.isNull()
        )

        error_columns.append(
            F.when(
                conversion_failed,
                F.lit(f"INVALID_TYPE:{name}"),
            )
        )

    result = raw_df.select(
        *converted_columns,
        F.array(*error_columns).alias("_errors_raw"),
    )

    result = (
        result
        .withColumn(
            "_type_errors",
            F.filter(
                F.col("_errors_raw"),
                lambda x: x.isNotNull(),
            ),
        )
        .drop("_errors_raw")
    )

    return result
