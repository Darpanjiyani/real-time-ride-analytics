"""Shared schema + config helpers for the pipeline source files.

The pipeline root folder (databricks/pipeline) is on sys.path when the
pipeline runs, so transformations can `from utilities.schemas import ...`.
"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RIDES_SCHEMA = StructType(
    [
        StructField("ride_id", StringType(), True),
        StructField("confirmation_number", StringType(), True),
        StructField("passenger_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("pickup_location_id", StringType(), True),
        StructField("dropoff_location_id", StringType(), True),
        StructField("vehicle_type_id", LongType(), True),
        StructField("vehicle_make_id", LongType(), True),
        StructField("payment_method_id", LongType(), True),
        StructField("ride_status_id", LongType(), True),
        StructField("pickup_city_id", LongType(), True),
        StructField("dropoff_city_id", LongType(), True),
        StructField("cancellation_reason_id", LongType(), True),
        StructField("passenger_name", StringType(), True),
        StructField("passenger_email", StringType(), True),
        StructField("passenger_phone", StringType(), True),
        StructField("driver_name", StringType(), True),
        StructField("driver_rating", DoubleType(), True),
        StructField("driver_phone", StringType(), True),
        StructField("driver_license", StringType(), True),
        StructField("vehicle_model", StringType(), True),
        StructField("vehicle_color", StringType(), True),
        StructField("license_plate", StringType(), True),
        StructField("pickup_address", StringType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("dropoff_address", StringType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("distance_miles", DoubleType(), True),
        StructField("duration_minutes", LongType(), True),
        # The original tutorial kept pickup/dropoff timestamps as strings;
        # all three are proper timestamps here.
        StructField("booking_timestamp", TimestampType(), True),
        StructField("pickup_timestamp", TimestampType(), True),
        StructField("dropoff_timestamp", TimestampType(), True),
        StructField("base_fare", DoubleType(), True),
        StructField("distance_fare", DoubleType(), True),
        StructField("time_fare", DoubleType(), True),
        StructField("surge_multiplier", DoubleType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_fare", DoubleType(), True),
        StructField("rating", DoubleType(), True),
    ]
)


def conform_to_rides_schema(df):
    """Select + cast columns so every source matches RIDES_SCHEMA exactly."""
    from pyspark.sql.functions import col

    return df.select([col(f.name).cast(f.dataType).alias(f.name) for f in RIDES_SCHEMA.fields])
