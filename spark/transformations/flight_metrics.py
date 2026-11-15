
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def minutes_between(end_column, start_column):
    """Return signed difference in minutes, preserving nulls."""
    return (
        F.col(end_column).cast("double")
        - F.col(start_column).cast("double")
    ) / F.lit(60.0)


def add_delay_metrics(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "departure_delay_minutes",
            minutes_between(
                "actual_departure",
                "scheduled_departure",
            ),
        )
        .withColumn(
            "arrival_delay_minutes",
            minutes_between(
                "actual_arrival",
                "scheduled_arrival",
            ),
        )
    )


def add_duration_metrics(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "scheduled_duration_minutes",
            minutes_between(
                "scheduled_arrival",
                "scheduled_departure",
            ),
        )
        .withColumn(
            "actual_duration_minutes",
            minutes_between(
                "actual_arrival",
                "actual_departure",
            ),
        )
    )


def add_on_time_indicators(
    df: DataFrame,
    threshold_minutes: int = 15,
) -> DataFrame:

    if threshold_minutes < 0:
        raise ValueError(
            "On-time threshold cannot be negative"
        )

    return (
        df
        .withColumn(
            "is_cancelled",
            F.col("flight_status") == "CANCELLED",
        )
        .withColumn(
            "is_departure_on_time",
            F.when(
                F.col("flight_status") == "CANCELLED",
                F.lit(None).cast("boolean"),
            ).when(
                F.col("departure_delay_minutes").isNull(),
                F.lit(None).cast("boolean"),
            ).otherwise(
                F.col("departure_delay_minutes")
                <= F.lit(threshold_minutes)
            ),
        )
        .withColumn(
            "is_arrival_on_time",
            F.when(
                F.col("flight_status") == "CANCELLED",
                F.lit(None).cast("boolean"),
            ).when(
                F.col("arrival_delay_minutes").isNull(),
                F.lit(None).cast("boolean"),
            ).otherwise(
                F.col("arrival_delay_minutes")
                <= F.lit(threshold_minutes)
            ),
        )
    )


def add_route_distance(
    flights: DataFrame,
    airports: DataFrame,
    earth_radius_km: float = 6371.0088,
) -> DataFrame:
    """Calculate great-circle distance using Haversine."""

    if earth_radius_km <= 0:
        raise ValueError("Earth radius must be positive")

    origin = airports.select(
        F.col("iata_code").alias("_origin_code"),
        F.col("latitude").alias("_origin_lat"),
        F.col("longitude").alias("_origin_lon"),
    )

    destination = airports.select(
        F.col("iata_code").alias("_destination_code"),
        F.col("latitude").alias("_destination_lat"),
        F.col("longitude").alias("_destination_lon"),
    )

    enriched = (
        flights
        .join(
            F.broadcast(origin),
            F.col("origin_airport") == F.col("_origin_code"),
            "left",
        )
        .join(
            F.broadcast(destination),
            F.col("destination_airport")
            == F.col("_destination_code"),
            "left",
        )
    )

    lat1 = F.radians(F.col("_origin_lat"))
    lat2 = F.radians(F.col("_destination_lat"))
    delta_lat = F.radians(
        F.col("_destination_lat") - F.col("_origin_lat")
    )
    delta_lon = F.radians(
        F.col("_destination_lon") - F.col("_origin_lon")
    )

    a = (
        F.pow(F.sin(delta_lat / 2), 2)
        + F.cos(lat1)
        * F.cos(lat2)
        * F.pow(F.sin(delta_lon / 2), 2)
    )

    # Clamp for floating-point rounding near antipodal points.
    a = F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))

    distance = (
        F.lit(2.0 * earth_radius_km)
        * F.asin(F.sqrt(a))
    )

    return (
        enriched
        .withColumn(
            "route_distance_km",
            F.round(distance, 2),
        )
        .drop(
            "_origin_code",
            "_origin_lat",
            "_origin_lon",
            "_destination_code",
            "_destination_lat",
            "_destination_lon",
        )
    )


def transform_flights(
    flights: DataFrame,
    airports: DataFrame,
    threshold_minutes: int = 15,
    earth_radius_km: float = 6371.0088,
) -> DataFrame:
    """Apply all flight metric transformations."""

    df = add_delay_metrics(flights)
    df = add_duration_metrics(df)

    df = add_on_time_indicators(
        df,
        threshold_minutes=threshold_minutes,
    )

    df = add_route_distance(
        df,
        airports,
        earth_radius_km=earth_radius_km,
    )

    return df
