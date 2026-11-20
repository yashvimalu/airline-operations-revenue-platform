
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PACKAGE_DIRS = [
    "spark",
    "ingestion",
    "data_quality",
    "scripts",
]

OUTPUT_DIR = PROJECT_ROOT / "dist"
OUTPUT_FILE = OUTPUT_DIR / "airline_app.zip"


def build_package():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with ZipFile(
        OUTPUT_FILE, "w", compression=ZIP_DEFLATED
    ) as archive:

        for directory in PACKAGE_DIRS:
            source_dir = PROJECT_ROOT / directory

            if not source_dir.is_dir():
                raise FileNotFoundError(
                    f"Required directory missing: {source_dir}"
                )

            for file_path in sorted(
                source_dir.rglob("*.py")
            ):
                # Exclude test caches and other generated files.
                if any(
                    part.startswith("__pycache__")
                    for part in file_path.parts
                ):
                    continue

                relative_path = file_path.relative_to(
                    PROJECT_ROOT
                )

                archive.write(
                    file_path,
                    arcname=relative_path.as_posix(),
                )

    print(f"Package created: {OUTPUT_FILE}")
    print(
        f"Size: {OUTPUT_FILE.stat().st_size:,} bytes"
    )


if __name__ == "__main__":
    build_package()
