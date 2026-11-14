
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

from spark.utils.schema_loader import (
    build_spark_schema,
    parse_spark_type,
)

from spark.transformations.schema_enforcement import apply_schema


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("SchemaEnforcementTests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture
def test_schema():
    return StructType([
        StructField("flight_id", StringType(), False),
        StructField("capacity", IntegerType(), False),
        StructField("scheduled_departure", TimestampType(), False),
    ])


def test_integer_type_mapping():
    assert isinstance(
        parse_spark_type("integer"),
        IntegerType,
    )


def test_unknown_type_rejected():
    with pytest.raises(ValueError, match="Unsupported"):
        parse_spark_type("unknown_type")


def test_build_schema_from_contract():
    contract = {
        "fields": [
            {
                "name": "flight_id",
                "type": "string",
                "nullable": False,
            },
            {
                "name": "capacity",
                "type": "integer",
                "nullable": False,
            },
        ]
    }

    schema = build_spark_schema(contract)

    assert schema.fieldNames() == ["flight_id", "capacity"]
    assert isinstance(schema["capacity"].dataType, IntegerType)
    assert schema["flight_id"].nullable is False


def test_valid_data_conversion(spark, test_schema):
    raw_df = spark.createDataFrame(
        [
            ("FLT001", "180", "2026-01-01T10:00:00+00:00"),
        ],
        ["flight_id", "capacity", "scheduled_departure"],
    )

    result = apply_schema(raw_df, test_schema).collect()[0]

    assert result["capacity"] == 180
    assert result["_type_errors"] == []
    assert result["scheduled_departure"] is not None


def test_invalid_integer_detected(spark, test_schema):
    raw_df = spark.createDataFrame(
        [
            ("FLT002", "ABC", "2026-01-01T10:00:00+00:00"),
        ],
        ["flight_id", "capacity", "scheduled_departure"],
    )

    result = apply_schema(raw_df, test_schema).collect()[0]

    assert result["capacity"] is None
    assert "INVALID_TYPE:capacity" in result["_type_errors"]


def test_invalid_timestamp_detected(spark, test_schema):
    raw_df = spark.createDataFrame(
        [
            ("FLT003", "180", "not-a-timestamp"),
        ],
        ["flight_id", "capacity", "scheduled_departure"],
    )

    result = apply_schema(raw_df, test_schema).collect()[0]

    assert result["scheduled_departure"] is None
    assert (
        "INVALID_TYPE:scheduled_departure"
        in result["_type_errors"]
    )


def test_missing_column_rejected(spark, test_schema):
    raw_df = spark.createDataFrame(
        [("FLT001", "180")],
        ["flight_id", "capacity"],
    )

    with pytest.raises(ValueError, match="Missing required"):
        apply_schema(raw_df, test_schema)

def test_flight_contract_schema():
    schema = load_flight_schema()

    assert len(schema.fields) == 14
    assert schema["flight_id"].nullable is False
    assert schema["actual_departure"].nullable is True
    assert schema["capacity"].dataType.simpleString() == "int"
    assert schema["flight_date"].dataType.simpleString() == "date"
    assert (
        schema["scheduled_departure"]
        .dataType.simpleString() == "timestamp"
    )