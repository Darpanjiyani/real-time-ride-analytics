"""Silver: parsed, validated rides.

* stg_rides       one streaming table fed by two append flows:
                  - a one-time historical backfill (bronze.bulk_rides)
                  - the live Event Hubs stream (bronze.rides_raw)
                  Data-quality expectations drop bad rows and record warnings.
* rides_quarantine  events whose JSON could not be parsed, kept for debugging
                  instead of being silently lost.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json

from utilities.schemas import RIDES_SCHEMA, conform_to_rides_schema

CATALOG = spark.conf.get("catalog", "rides")

# Rows failing these are DROPPED from silver (counts show in the pipeline UI)
HARD_RULES = {
    "valid_ride_id": "ride_id IS NOT NULL",
    "valid_booking_timestamp": "booking_timestamp IS NOT NULL",
    "non_negative_fare": "total_fare >= 0",
    "positive_distance": "distance_miles > 0",
}

# Rows failing these are KEPT but counted as warnings
SOFT_RULES = {
    "rating_in_range": "rating IS NULL OR rating BETWEEN 1 AND 5",
    "cancelled_ride_has_reason": "ride_status_id <> 2 OR cancellation_reason_id IN (1, 2, 3)",
    "fare_adds_up": "abs(total_fare - (subtotal + tip_amount)) < 0.01",
    "dropoff_after_pickup": "dropoff_timestamp >= pickup_timestamp",
}

dp.create_streaming_table(
    name=f"{CATALOG}.silver.stg_rides",
    comment="Parsed rides from backfill + live stream, with data-quality expectations",
    expect_all_or_drop=HARD_RULES,
    expect_all=SOFT_RULES,
)


@dp.append_flow(target=f"{CATALOG}.silver.stg_rides", name="rides_backfill")
def rides_backfill():
    """Historical rides loaded once into bronze.bulk_rides by setup/01_load_bronze."""
    return conform_to_rides_schema(spark.readStream.table(f"{CATALOG}.bronze.bulk_rides"))


@dp.append_flow(target=f"{CATALOG}.silver.stg_rides", name="rides_live_stream")
def rides_live_stream():
    parsed = (
        spark.readStream.table(f"{CATALOG}.bronze.rides_raw")
        .withColumn("ride", from_json(col("rides"), RIDES_SCHEMA))
        .filter(col("ride.ride_id").isNotNull())
        .select("ride.*")
    )
    return conform_to_rides_schema(parsed)


@dp.table(
    name=f"{CATALOG}.silver.rides_quarantine",
    comment="Raw events that could not be parsed into the ride schema",
)
def rides_quarantine():
    return (
        spark.readStream.table(f"{CATALOG}.bronze.rides_raw")
        .withColumn("ride", from_json(col("rides"), RIDES_SCHEMA))
        .filter(col("ride.ride_id").isNull())
        .select("rides", "topic", "partition", "offset", "enqueued_at")
    )
