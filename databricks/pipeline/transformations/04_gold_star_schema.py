"""Gold: star schema built with Auto CDC flows.

Dimensions (SCD Type 1 - latest value wins):
    dim_passenger, dim_driver, dim_vehicle, dim_payment, dim_booking
Dimension (SCD Type 2 - full history):
    dim_location   (a city rename creates a new version, old one is end-dated)
Fact:
    fact_rides     one row per ride

Fixes vs. the original tutorial:
* sequence_by used the business key itself (e.g. sequence_by="passenger_id"),
  so Auto CDC could not tell which record was newer. Every SCD1 flow now
  sequences by booking_timestamp; dim_location sequences by the city's
  updated_at.
* the fact table's CDC key listed six columns; the grain is one ride, so the
  key is just ride_id.
* @dp.view is not part of the pipelines API - @dp.temporary_view is.
* dropDuplicates() on high-cardinality UUID keys kept unbounded streaming
  state; Auto CDC already upserts by key, so those calls are removed. The
  low-cardinality location source keeps its dedup (state stays tiny).
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date

CATALOG = spark.conf.get("catalog", "rides")
SILVER_OBT = f"{CATALOG}.silver.silver_obt"
GOLD = f"{CATALOG}.gold"

# name -> (business key, attribute columns)
SCD1_DIMENSIONS = {
    "dim_passenger": (
        "passenger_id",
        ["passenger_name", "passenger_email", "passenger_phone"],
    ),
    "dim_driver": (
        "driver_id",
        ["driver_name", "driver_rating", "driver_phone", "driver_license"],
    ),
    "dim_vehicle": (
        "vehicle_id",
        ["vehicle_make_id", "vehicle_make", "vehicle_type_id", "vehicle_type",
         "vehicle_model", "vehicle_color", "license_plate"],
    ),
    "dim_payment": (
        "payment_method_id",
        ["payment_method", "is_card", "requires_auth"],
    ),
    "dim_booking": (
        "ride_id",
        ["confirmation_number", "ride_status", "cancellation_reason",
         "pickup_address", "pickup_latitude", "pickup_longitude",
         "dropoff_address", "dropoff_latitude", "dropoff_longitude",
         "pickup_timestamp", "dropoff_timestamp"],
    ),
}


def build_scd1_dimension(name: str, key: str, attributes: list[str]) -> None:
    source_view = f"{name}_source"

    @dp.temporary_view(name=source_view)
    def _source():
        return spark.readStream.table(SILVER_OBT).select(key, *attributes, "booking_timestamp")

    dp.create_streaming_table(name=f"{GOLD}.{name}", comment=f"SCD Type 1 dimension keyed on {key}")
    dp.create_auto_cdc_flow(
        target=f"{GOLD}.{name}",
        source=source_view,
        keys=[key],
        sequence_by=col("booking_timestamp"),
        except_column_list=["booking_timestamp"],
        stored_as_scd_type=1,
    )


for dim_name, (dim_key, dim_attributes) in SCD1_DIMENSIONS.items():
    build_scd1_dimension(dim_name, dim_key, dim_attributes)


# ---------------------------------------------------------------- dim_location (SCD 2)
@dp.temporary_view(name="dim_location_source")
def dim_location_source():
    return (
        spark.readStream.table(SILVER_OBT)
        .select(
            col("pickup_city_id").alias("city_id"),
            col("pickup_city").alias("city"),
            col("pickup_state").alias("state"),
            col("pickup_region").alias("region"),
            col("pickup_city_updated_at").alias("updated_at"),
        )
        # ~10 cities x a few versions, so this dedup state stays small
        .dropDuplicates(["city_id", "updated_at"])
    )


dp.create_streaming_table(
    name=f"{GOLD}.dim_location",
    comment="SCD Type 2 city dimension; current rows have __END_AT IS NULL",
)
dp.create_auto_cdc_flow(
    target=f"{GOLD}.dim_location",
    source="dim_location_source",
    keys=["city_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["city", "state", "region"],
)


# ------------------------------------------------------------------- fact_rides
@dp.temporary_view(name="fact_rides_source")
def fact_rides_source():
    return spark.readStream.table(SILVER_OBT).select(
        "ride_id",
        "passenger_id",
        "driver_id",
        "vehicle_id",
        "payment_method_id",
        col("pickup_city_id").alias("city_id"),
        "dropoff_city_id",
        "ride_status_id",
        "booking_timestamp",
        to_date("booking_timestamp").alias("booking_date"),
        "distance_miles",
        "duration_minutes",
        "base_fare",
        "distance_fare",
        "time_fare",
        "surge_multiplier",
        "subtotal",
        "tip_amount",
        "total_fare",
        "rating",
    )


dp.create_streaming_table(
    name=f"{GOLD}.fact_rides",
    comment="One row per ride with fare and trip measures",
    cluster_by=["booking_date", "city_id"],
)
dp.create_auto_cdc_flow(
    target=f"{GOLD}.fact_rides",
    source="fact_rides_source",
    keys=["ride_id"],
    sequence_by=col("booking_timestamp"),
    stored_as_scd_type=1,
)
