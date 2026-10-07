"""Publish ride events to Azure Event Hubs.

One producer client is created and reused, and events are sent in batches
(the original tutorial opened and closed a new client for every single ride).

Set DRY_RUN=true (or pass --dry-run) to print events instead of sending them,
so the producer can be tested locally without an Azure subscription.

Usage:
    python -m producer.event_hub_producer --count 100 --batch-size 25
    python -m producer.event_hub_producer --count 500 --rate 10   # ~10 events/sec
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Iterable

from dotenv import load_dotenv

from producer.data_generator import generate_ride

load_dotenv()
logger = logging.getLogger(__name__)


def _env_flag(name: str) -> bool:
    return os.getenv(name, "false").strip().lower() in {"1", "true", "yes"}


class RideEventProducer:
    """Thin wrapper around EventHubProducerClient with batching and dry-run."""

    def __init__(
        self,
        connection_string: str | None = None,
        eventhub_name: str | None = None,
        dry_run: bool | None = None,
    ) -> None:
        self.dry_run = _env_flag("DRY_RUN") if dry_run is None else dry_run
        self._client = None

        if self.dry_run:
            logger.info("DRY_RUN enabled: events will be printed, not sent")
            return

        connection_string = connection_string or os.getenv("EVENTHUB_CONNECTION_STRING")
        eventhub_name = eventhub_name or os.getenv("EVENTHUB_NAME")
        if not connection_string or not eventhub_name:
            raise ValueError(
                "EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME must be set "
                "(copy .env.example to .env) or run with DRY_RUN=true"
            )

        from azure.eventhub import EventHubProducerClient  # imported lazily for dry-run

        self._client = EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=eventhub_name
        )

    def send(self, rides: Iterable[dict]) -> int:
        """Send rides in as few batches as possible. Returns number of events sent."""
        rides = list(rides)
        if not rides:
            return 0

        if self.dry_run:
            for ride in rides:
                print(json.dumps(ride))
            return len(rides)

        from azure.eventhub import EventData

        sent = 0
        batch = self._client.create_batch()
        for ride in rides:
            event = EventData(json.dumps(ride))
            try:
                batch.add(event)
            except ValueError:  # batch is full -> flush and start a new one
                self._client.send_batch(batch)
                sent += len(batch)
                batch = self._client.create_batch()
                batch.add(event)
        if len(batch):
            self._client.send_batch(batch)
            sent += len(batch)
        logger.info("Sent %d events to Event Hubs", sent)
        return sent

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> "RideEventProducer":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream synthetic ride events")
    parser.add_argument("--count", type=int, default=10, help="total events to send")
    parser.add_argument("--batch-size", type=int, default=25, help="events per send call")
    parser.add_argument("--rate", type=float, default=0, help="target events/sec (0 = as fast as possible)")
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    total = 0
    started = time.perf_counter()
    with RideEventProducer(dry_run=args.dry_run or None) as producer:
        while total < args.count:
            n = min(args.batch_size, args.count - total)
            total += producer.send(generate_ride() for _ in range(n))
            if args.rate > 0:
                expected_elapsed = total / args.rate
                sleep_for = expected_elapsed - (time.perf_counter() - started)
                if sleep_for > 0:
                    time.sleep(sleep_for)

    elapsed = time.perf_counter() - started
    logger.info("Done: %d events in %.1fs (%.1f events/sec)", total, elapsed, total / max(elapsed, 1e-9))


if __name__ == "__main__":
    main()
