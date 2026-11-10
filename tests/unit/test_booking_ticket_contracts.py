from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_DIR = PROJECT_ROOT / "docs" / "data_contracts"


def load_contract(dataset):
    path = CONTRACT_DIR / f"{dataset}.yaml"

    with path.open(encoding="utf-8") as file:
        return yaml.safe_load(file)


@pytest.mark.parametrize("dataset", ["bookings", "tickets"])
def test_contract_structure(dataset):
    contract = load_contract(dataset)

    assert contract["dataset"] == dataset
    assert contract["version"] == "1.0.0"
    assert contract["schema"]
    assert contract["primary_key"]
    assert contract["validation_rules"]


@pytest.mark.parametrize("dataset", ["bookings", "tickets"])
def test_primary_key_is_not_nullable(dataset):
    contract = load_contract(dataset)

    fields = {
        field["name"]: field
        for field in contract["schema"]
    }

    for key in contract["primary_key"]:
        assert key in fields
        assert fields[key]["nullable"] is False


def test_ticket_foreign_keys():
    contract = load_contract("tickets")

    references = {
        (
            foreign_key["column"],
            foreign_key["references"]["dataset"],
            foreign_key["references"]["column"],
        )
        for foreign_key in contract["foreign_keys"]
    }

    assert ("booking_id", "bookings", "booking_id") in references
    assert (
        "flight_id",
        "flight_operations",
        "flight_id",
    ) in references


def test_ticket_status_values():
    contract = load_contract("tickets")

    statuses = contract["validation_rules"]["allowed_values"]["ticket_status"]

    assert {"ISSUED", "CANCELLED", "REFUNDED"} == set(statuses)