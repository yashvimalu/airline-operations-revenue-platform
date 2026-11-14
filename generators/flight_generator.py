
import csv
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.config_loader import load_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]

FLIGHT_FIELDS = [
    "flight_id",
    "flight_number",
    "airline_code",
    "origin_airport",
    "destination_airport",
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
    "flight_status",
    "aircraft_type",
    "capacity",
    "flight_date",
    "ingestion_timestamp",
]

AIRLINES = ["AI", "6E", "QP", "SG"]

AIRCRAFT = {
    "A320": 180,
    "A321": 220,
    "B737": 189,
    "B787": 256,
}


def load_airports(file_path):
    """Read real airport reference data."""
    airports = []

    with open(file_path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            airports.append({
                "iata_code": row["iata_code"],
                "latitude": float(row["latitude_deg"]),
                "longitude": float(row["longitude_deg"]),
            })

    if len(airports) < 2:
        raise ValueError("At least two airports are required")

    return airports


def calculate_distance(origin, destination):
    """Calculate great-circle distance in kilometres."""
    radius = 6371

    lat1 = math.radians(origin["latitude"])
    lon1 = math.radians(origin["longitude"])
    lat2 = math.radians(destination["latitude"])
    lon2 = math.radians(destination["longitude"])

    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(delta_lon / 2) ** 2
    )

    a = min(1.0, max(0.0, a))

    return 2 * radius * math.asin(math.sqrt(a))


def generate_flights(
    count,
    seed,
    airports,
    start_date,
    end_date,
):
    """Generate deterministic synthetic flight operations data."""
    if type(count) is not int or count <= 0:
        raise ValueError("count must be a positive integer")

    rng = random.Random(seed)

    start = datetime.fromisoformat(start_date).replace(
        tzinfo=timezone.utc
    )
    end = datetime.fromisoformat(end_date).replace(
        tzinfo=timezone.utc
    )

    if end < start:
        raise ValueError("end_date cannot precede start_date")

    day_range = (end.date() - start.date()).days

    # Fixed metadata timestamp for reproducible CSV output.
    ingestion_time = "2026-04-01T00:00:00+00:00"

    for index in range(1, count + 1):
        origin, destination = rng.sample(airports, 2)

        distance_km = calculate_distance(origin, destination)

        # Approximate cruise speed plus ground operations.
        duration_minutes = max(
            45,
            round((distance_km / 750) * 60 + 30),
        )

        scheduled_departure = start + timedelta(
            days=rng.randint(0, day_range),
            hours=rng.randint(0, 23),
            minutes=rng.choice([0, 15, 30, 45]),
        )

        scheduled_arrival = scheduled_departure + timedelta(
            minutes=duration_minutes
        )

        airline = rng.choice(AIRLINES)
        aircraft = rng.choice(list(AIRCRAFT))

        status = rng.choices(
            [
                "COMPLETED",
                "DELAYED",
                "CANCELLED",
                "SCHEDULED",
                "DEPARTED",
            ],
            weights=[70, 10, 5, 10, 5],
            k=1,
        )[0]

        actual_departure = None
        actual_arrival = None

        if status in ("COMPLETED", "DELAYED", "DEPARTED"):
            delay = (
                rng.randint(15, 120)
                if status == "DELAYED"
                else rng.randint(0, 14)
            )

            actual_departure = scheduled_departure + timedelta(
                minutes=delay
            )

            if status == "COMPLETED":
                actual_arrival = actual_departure + timedelta(
                    minutes=duration_minutes
                    + rng.randint(-10, 15)
                )

        yield {
            "flight_id": f"FLT{index:08d}",
            "flight_number": f"{airline}{rng.randint(100, 9999)}",
            "airline_code": airline,
            "origin_airport": origin["iata_code"],
            "destination_airport": destination["iata_code"],
            "scheduled_departure": scheduled_departure.isoformat(),
            "actual_departure": (
                actual_departure.isoformat()
                if actual_departure else None
            ),
            "scheduled_arrival": scheduled_arrival.isoformat(),
            "actual_arrival": (
                actual_arrival.isoformat()
                if actual_arrival else None
            ),
            "flight_status": status,
            "aircraft_type": aircraft,
            "capacity": AIRCRAFT[aircraft],
            "flight_date": scheduled_departure.date().isoformat(),
            "ingestion_timestamp": ingestion_time,
        }


def write_csv(flights, output_path):
    """Stream flight records to CSV without storing all rows in memory."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0

    with output_path.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=FLIGHT_FIELDS,
        )
        writer.writeheader()

        for flight in flights:
            writer.writerow(flight)
            count += 1

    return count


def main():
    config = load_config()

    generation = config["data_generation"]
    storage = config["storage"]

    if generation["output_format"] != "csv":
        raise ValueError(
            "AIR-12 currently supports CSV output only"
        )

    airport_path = (
        PROJECT_ROOT / storage["airports_reference_path"]
    )

    output_path = (
        PROJECT_ROOT / storage["raw_path"] / "flights.csv"
    )

    airports = load_airports(airport_path)

    flights = generate_flights(
        count=generation["flight_count"],
        seed=generation["seed"],
        airports=airports,
        start_date=generation["start_date"],
        end_date=generation["end_date"],
    )

    total = write_csv(flights, output_path)

    print(f"Loaded {len(airports)} airports")
    print(f"Generated {total:,} flights")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
