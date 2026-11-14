
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def load_validation_rules(contract):
    """Extract validation rules from the YAML contract."""
    rules = contract.get("validation_rules", {})

    if not isinstance(rules, dict):
        raise ValueError(
            "validation_rules must be a mapping"
        )

    return rules


def _add_error(df, condition, error_code):
    """Append an error code when a rule is violated."""
    return df.withColumn(
        "_validation_errors",
        F.when(
            condition,
            F.concat(
                F.col("_validation_errors"),
                F.array(F.lit(error_code)),
            ),
        ).otherwise(F.col("_validation_errors")),
    )


def validate_flights(
    df: DataFrame,
    contract: dict,
) -> DataFrame:
    """Apply AIR-10 flight validation rules."""

    rules = load_validation_rules(contract)

    required_columns = set(
        rules.get("not_null", [])
    )

    required_columns.update(
        rules.get("allowed_values", {}).keys()
    )
    required_columns.update(
        rules.get("regex", {}).keys()
    )
    required_columns.update(
        rules.get("numeric_constraints", {}).keys()
    )

    required_columns.update({
        "origin_airport",
        "destination_airport",
        "scheduled_departure",
        "scheduled_arrival",
        "flight_status",
        "actual_departure",
        "actual_arrival",
    })

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing validation columns: {sorted(missing)}"
        )

    # Preserve AIR-16 conversion errors.
    if "_type_errors" in df.columns:
        df = df.withColumn(
            "_validation_errors",
            F.coalesce(
                F.col("_type_errors"),
                F.expr("cast(array() as array<string>)"),
            ),
        )
    else:
        df = df.withColumn(
            "_validation_errors",
            F.expr("cast(array() as array<string>)"),
        )

    # 1. Required fields
    for column in rules.get("not_null", []):
        invalid = F.col(column).isNull()

        df = _add_error(
            df,
            invalid,
            f"NULL_VALUE:{column}",
        )

    # 2. Allowed values
    for column, values in rules.get(
        "allowed_values", {}
    ).items():
        invalid = (
            F.col(column).isNotNull()
            & ~F.col(column).isin(values)
        )

        df = _add_error(
            df,
            invalid,
            f"INVALID_VALUE:{column}",
        )

    # 3. Regex rules
    for column, pattern in rules.get(
        "regex", {}
    ).items():
        invalid = (
            F.col(column).isNotNull()
            & ~F.col(column).rlike(pattern)
        )

        df = _add_error(
            df,
            invalid,
            f"INVALID_FORMAT:{column}",
        )

    # 4. Numeric constraints
    for column, constraints in rules.get(
        "numeric_constraints", {}
    ).items():
        if "min_exclusive" in constraints:
            invalid = (
                F.col(column).isNotNull()
                & (
                    F.col(column)
                    <= F.lit(constraints["min_exclusive"])
                )
            )

            df = _add_error(
                df,
                invalid,
                f"MIN_EXCLUSIVE:{column}",
            )

    # 5. Cross-field: different airports
    df = _add_error(
        df,
        (
            F.col("origin_airport").isNotNull()
            & F.col("destination_airport").isNotNull()
            & (
                F.col("origin_airport")
                == F.col("destination_airport")
            )
        ),
        "SAME_ORIGIN_DESTINATION",
    )

    # 6. Scheduled arrival after departure
    df = _add_error(
        df,
        (
            F.col("scheduled_departure").isNotNull()
            & F.col("scheduled_arrival").isNotNull()
            & (
                F.col("scheduled_arrival")
                <= F.col("scheduled_departure")
            )
        ),
        "INVALID_SCHEDULED_TIME_ORDER",
    )

    # 7. Cancelled flights cannot have actual times
    cancelled_invalid = (
        (F.col("flight_status") == "CANCELLED")
        & (
            F.col("actual_departure").isNotNull()
            | F.col("actual_arrival").isNotNull()
        )
    )

    df = _add_error(
        df,
        cancelled_invalid,
        "CANCELLED_HAS_ACTUAL_TIMES",
    )

    # 8. Completed flights require actual times
    completed_invalid = (
        (F.col("flight_status") == "COMPLETED")
        & (
            F.col("actual_departure").isNull()
            | F.col("actual_arrival").isNull()
        )
    )

    df = _add_error(
        df,
        completed_invalid,
        "COMPLETED_MISSING_ACTUAL_TIMES",
    )

    return df.withColumn(
        "is_valid",
        F.size(F.col("_validation_errors")) == 0,
    )


def split_valid_invalid(df: DataFrame):
    """Return valid and invalid flight DataFrames."""
    valid_df = df.filter(F.col("is_valid"))
    invalid_df = df.filter(~F.col("is_valid"))

    return valid_df, invalid_df
