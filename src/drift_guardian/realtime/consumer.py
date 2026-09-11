"""Kafka consumer для realtime-мониторинга data drift"""

from __future__ import annotations

import json
import logging
import signal
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from drift_guardian.exporters.prometheus_exporter import PrometheusExporter
from drift_guardian.realtime.config import Settings
from drift_guardian.realtime.engine_adapter import build_engine
from drift_guardian.realtime.event import KafkaEvent
from drift_guardian.realtime.realtime_monitor import RealTimeDriftMonitor

LOGGER = logging.getLogger(__name__)
RUNNING = True


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    global RUNNING
    LOGGER.info("received signal %s; shutting down", signum)
    RUNNING = False


def _has_event_time_error(exc: ValidationError) -> bool:
    return any(error.get("loc", (None,))[0] == "event_time" for error in exc.errors())


def _commit_poison_record(consumer: Consumer, message: Message) -> None:
    consumer.commit(message=message, asynchronous=False)


def main() -> None:
    """Запустить consumer и endpoint Prometheus"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings.from_env()
    settings.validate()

    exporter = PrometheusExporter()
    metrics_server, metrics_thread = exporter.start_http_server(
        settings.prometheus_port
    )

    monitor = RealTimeDriftMonitor(
        engine=build_engine(),
        window_size=settings.window_size,
        min_window_size=settings.min_window_size,
        analyze_every_n_events=settings.analyze_every_n_events,
        late_event_threshold_seconds=settings.late_event_threshold_seconds,
        stream_warning_lag_seconds=settings.stream_warning_lag_seconds,
        stream_critical_lag_seconds=settings.stream_critical_lag_seconds,
        invalid_event_rate_warning=settings.invalid_event_rate_warning,
        invalid_event_rate_critical=settings.invalid_event_rate_critical,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "client.id": "drift-consumer",
        }
    )
    consumer.subscribe([settings.kafka_topic])

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    LOGGER.info(
        "consumer started topic=%s bootstrap=%s metrics_port=%s",
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
        settings.prometheus_port,
    )

    try:
        while RUNNING:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                payload = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                LOGGER.warning("dropping malformed JSON at offset=%s", message.offset())
                exporter.record_invalid_event("json")
                _commit_poison_record(consumer, message)
                continue

            try:
                event = KafkaEvent.model_validate(payload)
            except ValidationError as exc:
                LOGGER.warning(
                    "dropping invalid event at offset=%s: %s",
                    message.offset(),
                    exc,
                )
                exporter.record_invalid_event("schema")
                if _has_event_time_error(exc):
                    monitor.record_invalid_event_time()
                    exporter.update_stream(monitor.stream_snapshot())
                _commit_poison_record(consumer, message)
                continue

            try:
                report = monitor.update(event)
                exporter.record_processed_event()
                exporter.update_stream(monitor.stream_snapshot())
                if report is not None:
                    exporter.update_report(report)

                # offset коммитим только после успешной обработки события
                consumer.store_offsets(message=message)
                consumer.commit(asynchronous=False)
            except Exception:
                exporter.record_processing_error("monitor")
                LOGGER.exception(
                    "fatal processing error at partition=%s offset=%s; "
                    "record is intentionally left uncommitted",
                    message.partition(),
                    message.offset(),
                )
                raise
    finally:
        consumer.close()
        metrics_server.shutdown()
        metrics_server.server_close()
        metrics_thread.join(timeout=5)
        LOGGER.info("consumer stopped")


if __name__ == "__main__":
    main()
