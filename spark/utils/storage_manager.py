
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.utils.parquet_writer import (
    write_partitioned_parquet,
    read_partitioned_parquet,
)


@dataclass(frozen=True)
class StoragePaths:
    curated: str
    quarantine: str


def build_storage_paths(config: dict) -> StoragePaths:
    storage = config["storage"]
    output = config.get("output", {})

    curated_root = storage["processed_path"].rstrip("/")
    quarantine_root = storage["quarantine_path"].rstrip("/")

    curated_name = output.get(
        "curated_dataset", "flights"
    )
    quarantine_name = output.get(
        "quarantine_dataset", "flights"
    )

    for name in (curated_name, quarantine_name):
        if (
            not name
            or "/" in name
            or "\\" in name
            or name in (".", "..")
        ):
            raise ValueError(
                f"Invalid dataset name: {name!r}"
            )

    return StoragePaths(
        curated=f"{curated_root}/{curated_name}",
        quarantine=f"{quarantine_root}/{quarantine_name}",
    )


def persist_flight_datasets(
    spark: SparkSession,
    curated_df: DataFrame,
    quarantine_df: DataFrame,
    config: dict,
) -> StoragePaths:
    """Persist and verify curated and quarantine outputs."""

    paths = build_storage_paths(config)

    mode = config.get("output", {}).get(
        "write_mode", "overwrite"
    )

    if paths.curated == paths.quarantine:
        raise ValueError(
            "Curated and quarantine paths must differ"
        )

    # Pre-write counts are also used for reconciliation.
    curated_count = curated_df.count()
    quarantine_count = quarantine_df.count()

    write_partitioned_parquet(
        curated_df,
        paths.curated,
        mode,
    )

    write_partitioned_parquet(
        quarantine_df,
        paths.quarantine,
        mode,
    )

    saved_curated = read_partitioned_parquet(
        spark, paths.curated
    )

    saved_quarantine = read_partitioned_parquet(
        spark, paths.quarantine
    )

    actual_curated = saved_curated.count()
    actual_quarantine = saved_quarantine.count()

    if actual_curated != curated_count:
        raise RuntimeError(
            "Curated count mismatch: "
            f"expected={curated_count}, "
            f"actual={actual_curated}"
        )

    if actual_quarantine != quarantine_count:
        raise RuntimeError(
            "Quarantine count mismatch: "
            f"expected={quarantine_count}, "
            f"actual={actual_quarantine}"
        )

    print("\n=== AIR-20 STORAGE VERIFICATION ===")
    print(f"Curated path: {paths.curated}")
    print(f"Curated records: {actual_curated:,}")
    print(f"Quarantine path: {paths.quarantine}")
    print(f"Quarantine records: {actual_quarantine:,}")

    return paths