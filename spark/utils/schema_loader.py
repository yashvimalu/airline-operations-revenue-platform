from pathlib import Path
import yaml
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DateType,
    TimestampType,
    DecimalType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

TYPE_MAPPING = {
    "string": StringType,
    "integer": IntegerType,
    "int": IntegerType,
    "long": LongType,
    "double": DoubleType,
    "date": DateType,
    "timestamp": TimestampType,
}


def parse_spark_type(type_name):
    """Convert a contract type into a Spark data type."""
    type_name = type_name.strip().lower()

    if type_name.startswith("decimal(") and type_name.endswith(")"):
        precision, scale = type_name[8:-1].split(",")

        return DecimalType(
            int(precision.strip()),
            int(scale.strip()),
        )

    if type_name not in TYPE_MAPPING:
        raise ValueError(
            f"Unsupported contract data type: {type_name}"
        )

    return TYPE_MAPPING[type_name]()


def load_contract(contract_path):
    """Load and normalize a YAML data contract."""
    path = Path(contract_path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Data contract not found: {path}"
        )

    with path.open("r", encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    if not isinstance(contract, dict):
        raise ValueError("Contract must be a YAML mapping")

    # Support the AIR-10 format:
    # schema:
    #   - name: flight_id
    #     type: string
    fields = contract.get("schema")

    # Support other contract formats
    if isinstance(fields, dict):
        fields = fields.get("fields", fields.get("columns"))

    if fields is None:
        fields = contract.get("fields")

    if fields is None:
        fields = contract.get("columns")

    if not isinstance(fields, list) or not fields:
        raise ValueError(
            "Contract must contain a non-empty schema list"
        )

    for field in fields:
        if not isinstance(field, dict):
            raise ValueError(
                "Each schema field must be a mapping"
            )

        if not all(key in field for key in ("name", "type")):
            raise ValueError(
                "Each field must define name and type"
            )

    # Normalize for build_spark_schema()
    contract["fields"] = fields

    return contract


def build_spark_schema(contract):
    """Build StructType from a contract definition."""
    fields = []
    names = set()

    for definition in contract["fields"]:
        name = definition["name"]

        if name in names:
            raise ValueError(
                f"Duplicate contract field: {name}"
            )

        names.add(name)

        data_type = parse_spark_type(
            definition["type"]
        )

        nullable = definition.get("nullable", True)

        if not isinstance(nullable, bool):
            raise ValueError(
                f"Invalid nullable value for {name}"
            )

        fields.append(
            StructField(
                name,
                data_type,
                nullable=nullable,
            )
        )

    return StructType(fields)


def load_flight_schema():
    contract_path = (
        PROJECT_ROOT
        / "docs"
        / "data_contracts"
        / "flight_operations.yaml"
    )

    contract = load_contract(contract_path)

    return build_spark_schema(contract)
