
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class ReconciliationResult:
    raw_count: int
    curated_count: int
    quality_rejected_count: int
    duplicate_count: int
    quarantine_count: int


def reconcile_flights(
    raw_df: DataFrame,
    curated_df: DataFrame,
    invalid_df: DataFrame,
    duplicate_df: DataFrame,
    quarantine_df: DataFrame,
) -> ReconciliationResult:

    result = ReconciliationResult(
        raw_count=raw_df.count(),
        curated_count=curated_df.count(),
        quality_rejected_count=invalid_df.count(),
        duplicate_count=duplicate_df.count(),
        quarantine_count=quarantine_df.count(),
    )

    if result.raw_count != (
        result.curated_count + result.quarantine_count
    ):
        raise ValueError(
            "Raw-to-output reconciliation failed: "
            f"{result}"
        )

    if result.quarantine_count != (
        result.quality_rejected_count
        + result.duplicate_count
    ):
        raise ValueError(
            "Quarantine reconciliation failed: "
            f"{result}"
        )

    return result
