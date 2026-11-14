import csv
from datetime import datetime
import pytest
from generators.flight_generator import (
    AIRCRAFT,
    FLIGHT_FIELDS,
    calculate_distance,
    generate_flights,
    load_airports,
    write_csv,
)


@pytest.fixture
def sample_airports():
    return [
        {
            "iata_code": "BOM",
            "latitude": 19.0896,
            "longitude": 72.8656,
        },
        {
            "iata_code": "DEL",
            "latitude": 28.5562,
            "longitude": 77.1000,
        },
        {
            "iata_code": "BLR",
            "latitude": 13.1986,
            "longitude": 77.7066,
        },
    ]


@pytest.fixture
def generated_flights(sample_airports):
    return list(
        generate_flights(
            count=100,
            seed=42,
            airports=sample_airports,
            start_date="2026-01-01",
            end_date="2026-01-31",
        )
    )


def test_load_airports(tmp_path):
    path = tmp_path / "airports.csv"

    path.write_text(
        "iata_code,latitude_deg,longitude_deg\n"
        "BOM,19.0896,72.8656\n"
        "DEL,28.5562,77.1000\n",
        encoding="utf-8",
    )

    airports = load_airports(path)

    assert len(airports) == 2
    assert airports[0]["iata_code"] == "BOM"
    assert airports[1]["latitude"] == pytest.approx(28.5562)


def test_load_airports_requires_two_airports(tmp_path):
    path = tmp_path / "airports.csv"

    path.write_text(
        "iata_code,latitude_deg,longitude_deg\n"
        "BOM,19.0896,72.8656\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="At least two airports"):
        load_airports(path)


def test_distance_between_same_airport(sample_airports):
    distance = calculate_distance(
        sample_airports[0],
        sample_airports[0],
    )

    assert distance == pytest.approx(0)


def test_distance_between_mumbai_and_delhi(sample_airports):
    distance = calculate_distance(
        sample_airports[0],
        sample_airports[1],
    )

    assert 1000 < distance < 1300


def test_generated_record_count(generated_flights):
    assert len(generated_flights) == 100


def test_generated_columns(generated_flights):
    for flight in generated_flights:
        assert set(flight.keys()) == set(FLIGHT_FIELDS)


def test_flight_ids_are_unique(generated_flights):
    ids = [flight["flight_id"] for flight in generated_flights]

    assert len(ids) == len(set(ids))


def test_origin_and_destination_differ(generated_flights):
    for flight in generated_flights:
        assert flight["origin_airport"] != flight["destination_airport"]


def test_scheduled_arrival_after_departure(generated_flights):
    for flight in generated_flights:
        departure = datetime.fromisoformat(
            flight["scheduled_departure"]
        )
        arrival = datetime.fromisoformat(
            flight["scheduled_arrival"]
        )

        assert arrival > departure


def test_flight_dates_match_departure(generated_flights):
    for flight in generated_flights:
        departure = datetime.fromisoformat(
            flight["scheduled_departure"]
        )

        assert flight["flight_date"] == departure.date().isoformat()


def test_aircraft_capacity(generated_flights):
    for flight in generated_flights:
        assert flight["capacity"] == AIRCRAFT[flight["aircraft_type"]]
        assert flight["capacity"] > 0


def test_cancelled_flights_have_no_actual_timestamps(generated_flights):
    for flight in generated_flights:
        if flight["flight_status"] == "CANCELLED":
            assert flight["actual_departure"] is None
            assert flight["actual_arrival"] is None


def test_completed_flights_have_actual_timestamps(generated_flights):
    for flight in generated_flights:
        if flight["flight_status"] == "COMPLETED":
            assert flight["actual_departure"] is not None
            assert flight["actual_arrival"] is not None


def test_actual_arrival_after_actual_departure(generated_flights):
    for flight in generated_flights:
        if flight["actual_arrival"] is not None:
            departure = datetime.fromisoformat(
                flight["actual_departure"]
            )
            arrival = datetime.fromisoformat(
                flight["actual_arrival"]
            )

            assert arrival > departure


def test_generator_is_reproducible(sample_airports):
    arguments = {
        "count": 50,
        "seed": 42,
        "airports": sample_airports,
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }

    first_run = list(generate_flights(**arguments))
    second_run = list(generate_flights(**arguments))

    assert first_run == second_run


@pytest.mark.parametrize("count", [0, -1, 1.5, "100", True])
def test_invalid_record_count(count, sample_airports):
    with pytest.raises(ValueError, match="positive integer"):
        list(
            generate_flights(
                count=count,
                seed=42,
                airports=sample_airports,
                start_date="2026-01-01",
                end_date="2026-01-31",
            )
        )


def test_invalid_date_range(sample_airports):
    with pytest.raises(ValueError, match="end_date"):
        list(
            generate_flights(
                count=10,
                seed=42,
                airports=sample_airports,
                start_date="2026-02-01",
                end_date="2026-01-01",
            )
        )


def test_write_csv(tmp_path, generated_flights):
    output_path = tmp_path / "raw" / "flights.csv"

    count = write_csv(iter(generated_flights), output_path)

    assert output_path.exists()
    assert count == 100

    with output_path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    assert reader.fieldnames == FLIGHT_FIELDS
    assert len(rows) == 100
    assert rows[0]["flight_id"] == generated_flights[0]["flight_id"]
