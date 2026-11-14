import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE = Path.home()  # Replaced via command-line argument

FIELDS = [
    "iata_code",
    "name",
    "municipality",
    "iso_country",
    "latitude_deg",
    "longitude_deg",
]


def prepare_airports(source_path, output_path):
    airports = {}

    with open(source_path, newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)

        for row in reader:
            code = row["iata_code"].strip()

            if (
                row["iso_country"] != "IN"
                or row["scheduled_service"] != "yes"
                or row["type"] not in ("large_airport", "medium_airport")
                or len(code) != 3
                or not code.isalpha()
                or not code.isupper()
                or not row["latitude_deg"]
                or not row["longitude_deg"]
            ):
                continue

            airports.setdefault(
                code,
                {field: row[field] for field in FIELDS},
            )

    if len(airports) < 2:
        raise ValueError("Insufficient eligible airports")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(
            airports[code] for code in sorted(airports)
        )

    print(f"Prepared {len(airports)} airports: {output_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "data/sample/airports_reference.csv"),
    )
    args = parser.parse_args()

    prepare_airports(args.source, args.output)