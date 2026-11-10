from pathlib import Path
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONTRACT_PATH = (
    PROJECT_ROOT
    / "docs"
    / "data_contracts"
    / "flight_operations.yaml"
)


def test_contract_exists():
    assert CONTRACT_PATH.is_file()


def test_contract_has_required_fields():
    with CONTRACT_PATH.open(encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    assert contract["dataset"] == "flight_operations"
    assert contract["version"] == "1.0.0"

    fields = {column["name"] for column in contract["schema"]}

    required = {
        "flight_id",
        "flight_number",
        "origin_airport",
        "destination_airport",
        "scheduled_departure",
        "scheduled_arrival",
        "flight_status",
        "capacity",
        "flight_date",
    }

    assert required.issubset(fields)


def test_primary_key_is_not_nullable():
    with CONTRACT_PATH.open(encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    columns = {
        column["name"]: column
        for column in contract["schema"]
    }

    for key in contract["primary_key"]:
        assert columns[key]["nullable"] is False


def test_flight_status_values_defined():
    with CONTRACT_PATH.open(encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    statuses = contract["validation_rules"]["allowed_values"]["flight_status"]

    assert "COMPLETED" in statuses
    assert "CANCELLED" in statuses