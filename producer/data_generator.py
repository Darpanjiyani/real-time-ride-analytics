"""Synthetic ride-event generator.

Produces one JSON-serialisable ride confirmation per call. Lookup IDs line up
with the files in ``data/`` so the Databricks silver layer can join them.

Changes vs. the original tutorial generator:
* fares use the selected vehicle type's base / per-mile / per-minute rates
* ride status and cancellation reason are consistent (a cancelled ride always
  has a reason, a completed ride never does) and cancelled rides carry no fare
* pickup / dropoff coordinates fall near the chosen city, not anywhere on Earth
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from typing import Any

from faker import Faker

fake = Faker()

VEHICLE_TYPES = [
    {"vehicle_type_id": 1, "vehicle_type": "UberX", "base_rate": 2.50, "per_mile": 1.75, "per_minute": 0.35},
    {"vehicle_type_id": 2, "vehicle_type": "UberXL", "base_rate": 3.50, "per_mile": 2.25, "per_minute": 0.45},
    {"vehicle_type_id": 3, "vehicle_type": "UberPOOL", "base_rate": 2.00, "per_mile": 1.50, "per_minute": 0.30},
    {"vehicle_type_id": 4, "vehicle_type": "Uber Comfort", "base_rate": 3.00, "per_mile": 2.00, "per_minute": 0.40},
    {"vehicle_type_id": 5, "vehicle_type": "Uber Black", "base_rate": 5.00, "per_mile": 3.50, "per_minute": 0.60},
]

PAYMENT_METHOD_IDS = {"Credit Card": 1, "Debit Card": 2, "Digital Wallet": 3, "Cash": 4}
RIDE_STATUS_IDS = {"Completed": 1, "Cancelled": 2}
VEHICLE_MAKE_IDS = {"Toyota": 1, "Honda": 2, "Ford": 3, "Chevrolet": 4, "Nissan": 5, "BMW": 6, "Mercedes": 7}
CANCELLATION_REASON_IDS = {"Driver cancelled": 1, "Passenger cancelled": 2, "No show": 3}
COMPLETED_REASON_ID = 4  # "no cancellation" row in map_cancellation_reasons.json

# city_id -> (lat, lon) of the city centre; must match data/map_cities.json
CITY_CENTRES = {
    1: (40.7128, -74.0060),   # New York
    2: (34.0522, -118.2437),  # Los Angeles
    3: (41.8781, -87.6298),   # Chicago
    4: (29.7604, -95.3698),   # Houston
    5: (33.4484, -112.0740),  # Phoenix
    6: (39.9526, -75.1652),   # Philadelphia
    7: (29.4241, -98.4936),   # San Antonio
    8: (32.7157, -117.1611),  # San Diego
    9: (32.7767, -96.7970),   # Dallas
    10: (37.3382, -121.8863), # San Jose
}

CANCELLATION_RATE = 0.10


def _point_near(city_id: int, spread: float = 0.15) -> tuple[float, float]:
    lat, lon = CITY_CENTRES[city_id]
    return (
        round(lat + random.uniform(-spread, spread), 6),
        round(lon + random.uniform(-spread, spread), 6),
    )


def generate_ride(now: datetime | None = None) -> dict[str, Any]:
    """Return one synthetic ride confirmation event."""
    now = now or datetime.now()

    # Timestamps: booked "just now" so streaming events look live
    booking_time = now - timedelta(seconds=random.randint(0, 60))
    pickup_time = booking_time + timedelta(minutes=random.randint(2, 15))
    duration_minutes = random.randint(5, 90)
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)

    vehicle = random.choice(VEHICLE_TYPES)
    pickup_city_id = random.choice(list(CITY_CENTRES))
    # most trips stay in the same city
    dropoff_city_id = pickup_city_id if random.random() < 0.9 else random.choice(list(CITY_CENTRES))
    pickup_lat, pickup_lon = _point_near(pickup_city_id)
    dropoff_lat, dropoff_lon = _point_near(dropoff_city_id)

    distance = round(random.uniform(0.5, 40), 2)
    surge_multiplier = round(random.choice([1.0, 1.0, 1.0, random.uniform(1.1, 2.5)]), 2)

    is_cancelled = random.random() < CANCELLATION_RATE
    if is_cancelled:
        ride_status = "Cancelled"
        cancellation_reason_id = CANCELLATION_REASON_IDS[random.choice(list(CANCELLATION_REASON_IDS))]
        base_fare = distance_fare = time_fare = subtotal = tip = total_fare = 0.0
        rating = None
    else:
        ride_status = "Completed"
        cancellation_reason_id = COMPLETED_REASON_ID
        base_fare = vehicle["base_rate"]
        distance_fare = round(distance * vehicle["per_mile"], 2)
        time_fare = round(duration_minutes * vehicle["per_minute"], 2)
        subtotal = round((base_fare + distance_fare + time_fare) * surge_multiplier, 2)
        tip = round(random.choice([0, 0, 0, 1, 2, 3, 5, random.uniform(1, 20)]), 2)
        total_fare = round(subtotal + tip, 2)
        rating = random.choice([None, random.randint(1, 5)])

    vehicle_make = random.choice(list(VEHICLE_MAKE_IDS))

    return {
        # Keys
        "ride_id": str(uuid.uuid4()),
        "confirmation_number": fake.bothify("??#-####-??##"),
        "passenger_id": str(uuid.uuid4()),
        "driver_id": str(uuid.uuid4()),
        "vehicle_id": str(uuid.uuid4()),
        "pickup_location_id": str(uuid.uuid4()),
        "dropoff_location_id": str(uuid.uuid4()),
        # Foreign keys to lookup tables
        "vehicle_type_id": vehicle["vehicle_type_id"],
        "vehicle_make_id": VEHICLE_MAKE_IDS[vehicle_make],
        "payment_method_id": PAYMENT_METHOD_IDS[random.choice(list(PAYMENT_METHOD_IDS))],
        "ride_status_id": RIDE_STATUS_IDS[ride_status],
        "pickup_city_id": pickup_city_id,
        "dropoff_city_id": dropoff_city_id,
        "cancellation_reason_id": cancellation_reason_id,
        # Passenger
        "passenger_name": fake.name(),
        "passenger_email": fake.email(),
        "passenger_phone": fake.phone_number(),
        # Driver
        "driver_name": fake.name(),
        "driver_rating": round(random.uniform(4.0, 5.0), 2),
        "driver_phone": fake.phone_number(),
        "driver_license": fake.bothify("??-???-#######"),
        # Vehicle
        "vehicle_model": fake.word().capitalize(),
        "vehicle_color": random.choice(["Black", "White", "Gray", "Silver", "Blue", "Red"]),
        "license_plate": fake.bothify("???-####"),
        # Locations
        "pickup_address": fake.address().replace("\n", ", "),
        "pickup_latitude": pickup_lat,
        "pickup_longitude": pickup_lon,
        "dropoff_address": fake.address().replace("\n", ", "),
        "dropoff_latitude": dropoff_lat,
        "dropoff_longitude": dropoff_lon,
        # Ride measures
        "distance_miles": distance,
        "duration_minutes": duration_minutes,
        "booking_timestamp": booking_time.isoformat(),
        "pickup_timestamp": pickup_time.isoformat(),
        "dropoff_timestamp": dropoff_time.isoformat(),
        # Pricing
        "base_fare": base_fare,
        "distance_fare": distance_fare,
        "time_fare": time_fare,
        "surge_multiplier": surge_multiplier,
        "subtotal": subtotal,
        "tip_amount": tip,
        "total_fare": total_fare,
        "rating": rating,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(generate_ride(), indent=2))
