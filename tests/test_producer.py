import json

from fastapi.testclient import TestClient

from producer.data_generator import generate_ride
from producer.event_hub_producer import RideEventProducer


def test_dry_run_send_prints_every_event(capsys):
    with RideEventProducer(dry_run=True) as producer:
        sent = producer.send(generate_ride() for _ in range(7))
    lines = capsys.readouterr().out.strip().splitlines()
    assert sent == 7 and len(lines) == 7
    assert all(json.loads(line)["ride_id"] for line in lines)


def test_missing_credentials_raise(monkeypatch):
    monkeypatch.delenv("EVENTHUB_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("EVENTHUB_NAME", raising=False)
    try:
        RideEventProducer(dry_run=False)
    except ValueError as e:
        assert "EVENTHUB_CONNECTION_STRING" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_api_endpoints(monkeypatch):
    monkeypatch.setenv("DRY_RUN", "true")
    from producer.app import app

    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert "Book a Ride" in client.get("/").text
        assert "Ride ID" in client.get("/book").text
        assert client.post("/simulate?count=5").json() == {"events_sent": 5}
