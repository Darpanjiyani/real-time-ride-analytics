"""Generate the silver one-big-table SQL from a join config using Jinja.

Instead of hand-writing a 7-table join, the joins are declared once in
OBT_CONFIG and rendered into databricks/pipeline/transformations/03_silver_obt.sql.

    python tools/render_silver_obt.py          # (re)write the SQL file
    python tools/render_silver_obt.py --check  # fail if the file is stale (used in tests/CI)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from jinja2 import Template

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "databricks" / "pipeline" / "transformations" / "03_silver_obt.sql"

RIDE_COLUMNS = [
    "ride_id", "confirmation_number", "passenger_id", "driver_id", "vehicle_id",
    "pickup_location_id", "dropoff_location_id", "vehicle_type_id", "vehicle_make_id",
    "payment_method_id", "ride_status_id", "pickup_city_id", "dropoff_city_id",
    "cancellation_reason_id", "passenger_name", "passenger_email", "passenger_phone",
    "driver_name", "driver_rating", "driver_phone", "driver_license", "vehicle_model",
    "vehicle_color", "license_plate", "pickup_address", "pickup_latitude",
    "pickup_longitude", "dropoff_address", "dropoff_latitude", "dropoff_longitude",
    "distance_miles", "duration_minutes", "booking_timestamp", "pickup_timestamp",
    "dropoff_timestamp", "base_fare", "distance_fare", "time_fare", "surge_multiplier",
    "subtotal", "tip_amount", "total_fare", "rating",
]

# First entry is the streaming driver table; the rest are LEFT JOINed lookups.
OBT_CONFIG = [
    {
        "table": "${catalog}.silver.stg_rides",
        "alias": "r",
        "columns": [f"r.{c}" for c in RIDE_COLUMNS],
        "watermark": "booking_timestamp DELAY OF INTERVAL 3 MINUTES",
    },
    {
        "table": "${catalog}.bronze.map_vehicle_makes",
        "alias": "vm",
        "columns": ["vm.vehicle_make"],
        "on": "r.vehicle_make_id = vm.vehicle_make_id",
    },
    {
        "table": "${catalog}.bronze.map_vehicle_types",
        "alias": "vt",
        "columns": ["vt.vehicle_type", "vt.description AS vehicle_type_description",
                    "vt.base_rate", "vt.per_mile", "vt.per_minute"],
        "on": "r.vehicle_type_id = vt.vehicle_type_id",
    },
    {
        "table": "${catalog}.bronze.map_ride_statuses",
        "alias": "rs",
        "columns": ["rs.ride_status"],
        "on": "r.ride_status_id = rs.ride_status_id",
    },
    {
        "table": "${catalog}.bronze.map_payment_methods",
        "alias": "pm",
        "columns": ["pm.payment_method", "pm.is_card", "pm.requires_auth"],
        "on": "r.payment_method_id = pm.payment_method_id",
    },
    {
        "table": "${catalog}.bronze.map_cities",
        "alias": "pc",
        "columns": ["pc.city AS pickup_city", "pc.state AS pickup_state",
                    "pc.region AS pickup_region", "pc.updated_at AS pickup_city_updated_at"],
        "on": "r.pickup_city_id = pc.city_id",
    },
    {
        "table": "${catalog}.bronze.map_cancellation_reasons",
        "alias": "cr",
        "columns": ["cr.cancellation_reason"],
        "on": "r.cancellation_reason_id = cr.cancellation_reason_id",
    },
]

TEMPLATE = Template(
    """\
-- GENERATED FILE - edit tools/render_silver_obt.py and re-run it instead.
-- Silver one-big-table: each streamed ride enriched with every lookup table
-- (stream-static joins, so lookup updates such as a city rename are picked up
-- by new rides - this feeds the SCD Type 2 city dimension in gold).

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_obt
COMMENT 'Rides enriched with all lookup attributes'
AS
SELECT
{%- for cfg in config %}
  {%- for c in cfg.columns %}
  {{ c }}{{ "," if not (loop.last and loop_outer_last(cfg)) }}
  {%- endfor %}
{%- endfor %}
FROM STREAM({{ config[0].table }}) WATERMARK {{ config[0].watermark }} {{ config[0].alias }}
{%- for cfg in config[1:] %}
LEFT JOIN {{ cfg.table }} {{ cfg.alias }}
  ON {{ cfg.on }}
{%- endfor %}
"""
)


def render() -> str:
    last = OBT_CONFIG[-1]
    return TEMPLATE.render(config=OBT_CONFIG, loop_outer_last=lambda cfg: cfg is last) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="exit 1 if the SQL file is out of date")
    args = parser.parse_args()

    sql = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != sql:
            print(f"{OUTPUT.relative_to(ROOT)} is stale - run: python tools/render_silver_obt.py")
            return 1
        print("silver_obt SQL is up to date")
        return 0

    OUTPUT.write_text(sql)
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
