import json
from datetime import datetime
from pathlib import Path

import pytest

from producer.data_generator import CITY_CENTRES, VEHICLE_TYPES, generate_ride

DATA = Path(__file__).resolve().parents[1] / "data"
RIDES = [generate_ride() for _ in range(2000)]


def _ids(file, key):
    return {row[key] for row in json.loads((DATA / file).read_text())}


def test_all_fields_present_and_serialisable():
    expected = set(json.loads((DATA / "bulk_rides.json").read_text())[0])
    for ride in RIDES[:50]:
        assert set(ride) == expected
        json.dumps(ride)


@pytest.mark.parametrize(
    "field,file,key",
    [
        ("vehicle_type_id", "map_vehicle_types.json", "vehicle_type_id"),
        ("vehicle_make_id", "map_vehicle_makes.json", "vehicle_make_id"),
        ("payment_method_id", "map_payment_methods.json", "payment_method_id"),
        ("ride_status_id", "map_ride_statuses.json", "ride_status_id"),
        ("pickup_city_id", "map_cities.json", "city_id"),
        ("dropoff_city_id", "map_cities.json", "city_id"),
        ("cancellation_reason_id", "map_cancellation_reasons.json", "cancellation_reason_id"),
    ],
)
def test_foreign_keys_exist_in_lookup_files(field, file, key):
    valid = _ids(file, key)
    assert {r[field] for r in RIDES} <= valid


def test_cancellation_is_consistent():
    for r in RIDES:
        if r["ride_status_id"] == 2:
            assert r["cancellation_reason_id"] in (1, 2, 3)
            assert r["total_fare"] == 0 and r["rating"] is None
        else:
            assert r["cancellation_reason_id"] == 4


def test_fare_matches_vehicle_rates():
    rates = {v["vehicle_type_id"]: v for v in VEHICLE_TYPES}
    for r in RIDES:
        if r["ride_status_id"] == 2:
            continue
        v = rates[r["vehicle_type_id"]]
        assert r["base_fare"] == v["base_rate"]
        assert r["distance_fare"] == pytest.approx(r["distance_miles"] * v["per_mile"], abs=0.01)
        assert r["time_fare"] == pytest.approx(r["duration_minutes"] * v["per_minute"], abs=0.01)
        assert r["total_fare"] == pytest.approx(r["subtotal"] + r["tip_amount"], abs=0.01)


def test_timestamps_are_ordered():
    for r in RIDES:
        booking = datetime.fromisoformat(r["booking_timestamp"])
        pickup = datetime.fromisoformat(r["pickup_timestamp"])
        dropoff = datetime.fromisoformat(r["dropoff_timestamp"])
        assert booking < pickup < dropoff


def test_coordinates_near_pickup_city():
    for r in RIDES:
        lat, lon = CITY_CENTRES[r["pickup_city_id"]]
        assert abs(r["pickup_latitude"] - lat) <= 0.15
        assert abs(r["pickup_longitude"] - lon) <= 0.15


def test_rating_range():
    assert all(r["rating"] is None or 1 <= r["rating"] <= 5 for r in RIDES)
