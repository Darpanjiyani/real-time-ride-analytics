-- GENERATED FILE - edit tools/render_silver_obt.py and re-run it instead.
-- Silver one-big-table: each streamed ride enriched with every lookup table
-- (stream-static joins, so lookup updates such as a city rename are picked up
-- by new rides - this feeds the SCD Type 2 city dimension in gold).

CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.silver_obt
COMMENT 'Rides enriched with all lookup attributes'
AS
SELECT
  r.ride_id,
  r.confirmation_number,
  r.passenger_id,
  r.driver_id,
  r.vehicle_id,
  r.pickup_location_id,
  r.dropoff_location_id,
  r.vehicle_type_id,
  r.vehicle_make_id,
  r.payment_method_id,
  r.ride_status_id,
  r.pickup_city_id,
  r.dropoff_city_id,
  r.cancellation_reason_id,
  r.passenger_name,
  r.passenger_email,
  r.passenger_phone,
  r.driver_name,
  r.driver_rating,
  r.driver_phone,
  r.driver_license,
  r.vehicle_model,
  r.vehicle_color,
  r.license_plate,
  r.pickup_address,
  r.pickup_latitude,
  r.pickup_longitude,
  r.dropoff_address,
  r.dropoff_latitude,
  r.dropoff_longitude,
  r.distance_miles,
  r.duration_minutes,
  r.booking_timestamp,
  r.pickup_timestamp,
  r.dropoff_timestamp,
  r.base_fare,
  r.distance_fare,
  r.time_fare,
  r.surge_multiplier,
  r.subtotal,
  r.tip_amount,
  r.total_fare,
  r.rating,
  vm.vehicle_make,
  vt.vehicle_type,
  vt.description AS vehicle_type_description,
  vt.base_rate,
  vt.per_mile,
  vt.per_minute,
  rs.ride_status,
  pm.payment_method,
  pm.is_card,
  pm.requires_auth,
  pc.city AS pickup_city,
  pc.state AS pickup_state,
  pc.region AS pickup_region,
  pc.updated_at AS pickup_city_updated_at,
  cr.cancellation_reason
FROM STREAM(${catalog}.silver.stg_rides) WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES r
LEFT JOIN ${catalog}.bronze.map_vehicle_makes vm
  ON r.vehicle_make_id = vm.vehicle_make_id
LEFT JOIN ${catalog}.bronze.map_vehicle_types vt
  ON r.vehicle_type_id = vt.vehicle_type_id
LEFT JOIN ${catalog}.bronze.map_ride_statuses rs
  ON r.ride_status_id = rs.ride_status_id
LEFT JOIN ${catalog}.bronze.map_payment_methods pm
  ON r.payment_method_id = pm.payment_method_id
LEFT JOIN ${catalog}.bronze.map_cities pc
  ON r.pickup_city_id = pc.city_id
LEFT JOIN ${catalog}.bronze.map_cancellation_reasons cr
  ON r.cancellation_reason_id = cr.cancellation_reason_id
