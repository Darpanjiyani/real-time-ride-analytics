"""Gold: business-ready aggregates for dashboards.

city_hourly_metrics - rides, cancellations, revenue and surge per city per hour,
joined to the *current* version of the SCD2 location dimension.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "rides")
GOLD = f"{CATALOG}.gold"


@dp.materialized_view(
    name=f"{GOLD}.city_hourly_metrics",
    comment="Hourly ride volume, cancellation rate and revenue by city",
)
def city_hourly_metrics():
    fact = spark.read.table(f"{GOLD}.fact_rides")
    current_city = spark.read.table(f"{GOLD}.dim_location").filter(F.col("__END_AT").isNull())

    completed = F.col("ride_status_id") == 1
    return (
        fact.join(current_city, "city_id", "left")
        .groupBy(
            F.date_trunc("hour", "booking_timestamp").alias("booking_hour"),
            "city_id",
            "city",
            "region",
        )
        .agg(
            F.count("*").alias("total_rides"),
            F.sum(F.when(completed, 1).otherwise(0)).alias("completed_rides"),
            F.round(1 - F.sum(F.when(completed, 1).otherwise(0)) / F.count("*"), 4).alias("cancellation_rate"),
            F.round(F.sum(F.when(completed, F.col("total_fare"))), 2).alias("gross_revenue"),
            F.round(F.avg(F.when(completed, F.col("total_fare"))), 2).alias("avg_fare"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
        )
    )
