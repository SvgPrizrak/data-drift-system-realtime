"""Kafka producer для локального и демонстрационного потока событий"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from confluent_kafka import KafkaError, Message, Producer

from drift_guardian.realtime.config import Settings
from drift_guardian.realtime.event import KafkaEvent

LOGGER = logging.getLogger(__name__)
RUNNING = True


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    global RUNNING
    LOGGER.info("received signal %s; stopping producer", signum)
    RUNNING = False


def _delivery_report(error: KafkaError | None, message: Message) -> None:
    if error is not None:
        LOGGER.error("delivery failed: %s", error)
        return

    LOGGER.debug(
        "delivered topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def _synthetic_events(seed: int, drift_after: int) -> Iterator[dict[str, object]]:
    rng = random.Random(seed)
    event_id = 1

    while True:
        drifted = event_id > drift_after
        age = rng.gauss(52 if drifted else 38, 11)
        income = rng.gauss(125_000 if drifted else 85_000, 18_000)
        country = rng.choices(
            ["DE", "FR", "EE", "US"] if drifted else ["DE", "FR", "EE"],
            weights=[0.35, 0.25, 0.15, 0.25] if drifted else [0.55, 0.3, 0.15],
            k=1,
        )[0]
        prediction_score = rng.betavariate(4, 3) if drifted else rng.betavariate(2, 8)

        yield {
            "event_id": event_id,
            "event_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "age": max(18, min(80, round(age))),
            "income": max(0, round(income, 2)),
            "country": country,
            "prediction_score": round(prediction_score, 6),
        }
        event_id += 1


def _jsonl_events(path: Path) -> Iterator[dict[str, object]]:
    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"invalid JSONL at {path}:{line_number}: {exc}"
                ) from exc

            if not isinstance(payload, dict):
                raise ValueError(
                    f"JSONL record at {path}:{line_number} must be an object"
                )
            yield payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send feature events to Kafka")
    parser.add_argument(
        "--input-jsonl",
        type=Path,
        default=None,
        help="Optional JSONL file. If omitted, synthetic events are generated.",
    )
    return parser.parse_args()


def main() -> None:
    """Запустить Kafka producer и отправлять события до сигнала остановки"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings.from_env()
    settings.validate()
    args = _parse_args()

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "drift-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
        }
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    events = (
        _jsonl_events(args.input_jsonl)
        if args.input_jsonl
        else _synthetic_events(
            settings.producer_seed,
            settings.producer_drift_after_events,
        )
    )

    LOGGER.info(
        "producer started topic=%s bootstrap=%s source=%s",
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
        args.input_jsonl or "synthetic",
    )

    try:
        for raw_event in events:
            if not RUNNING:
                break

            event = KafkaEvent.model_validate(raw_event)
            body = event.model_dump_json().encode("utf-8")
            key = str(event.event_id).encode("utf-8")

            # при заполненном локальном буфере librdkafka освобождаем очередь poll'ом
            while RUNNING:
                try:
                    producer.produce(
                        topic=settings.kafka_topic,
                        key=key,
                        value=body,
                        on_delivery=_delivery_report,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)

            producer.poll(0)
            if settings.producer_interval_seconds > 0:
                time.sleep(settings.producer_interval_seconds)
    finally:
        remaining = producer.flush(timeout=10)
        if remaining:
            LOGGER.error("%s messages were not delivered before shutdown", remaining)
        LOGGER.info("producer stopped")


if __name__ == "__main__":
    main()
