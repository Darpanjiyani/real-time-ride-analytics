"""FastAPI ride-booking front end.

Every booking generates a ride event and publishes it to Event Hubs.

Run:
    uvicorn producer.app:app --reload
Then open http://127.0.0.1:8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.templating import Jinja2Templates

from producer.data_generator import generate_ride
from producer.event_hub_producer import RideEventProducer

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One producer for the lifetime of the app instead of one per request
    app.state.producer = RideEventProducer()
    yield
    app.state.producer.close()


app = FastAPI(title="Ride Booking Service", lifespan=lifespan)


@app.get("/")
def booking_home(request: Request):
    return templates.TemplateResponse(request, "home.html")


@app.get("/book")
def book_ride(request: Request):
    ride = generate_ride()
    request.app.state.producer.send([ride])
    return templates.TemplateResponse(request, "confirmation.html", {"ride": ride})


@app.post("/simulate")
def simulate(request: Request, count: int = Query(100, ge=1, le=10_000)):
    """Publish `count` rides in batches - handy for load-testing the pipeline."""
    sent = request.app.state.producer.send(generate_ride() for _ in range(count))
    return {"events_sent": sent}


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("producer.app:app", host="127.0.0.1", port=8000)
