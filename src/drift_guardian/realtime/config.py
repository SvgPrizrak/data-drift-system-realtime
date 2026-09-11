"""Настройки realtime-сервиса"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    """Настройки Kafka, sliding window и Prometheus"""

    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    kafka_auto_offset_reset: str
    prometheus_port: int
    window_size: int
    min_window_size: int
    analyze_every_n_events: int
    late_event_threshold_seconds: float
    stream_warning_lag_seconds: float
    stream_critical_lag_seconds: float
    invalid_event_rate_warning: float
    invalid_event_rate_critical: float
    producer_interval_seconds: float
    producer_seed: int
    producer_drift_after_events: int

    @classmethod
    def from_env(cls) -> Settings:
        """Загрузить настройки из переменных окружения"""
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:19092",
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", "features-stream"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "drift-consumer"),
            kafka_auto_offset_reset=os.getenv(
                "KAFKA_AUTO_OFFSET_RESET",
                "earliest",
            ),
            prometheus_port=int(os.getenv("PROMETHEUS_PORT", "8000")),
            window_size=int(os.getenv("WINDOW_SIZE", "1000")),
            min_window_size=int(os.getenv("MIN_WINDOW_SIZE", "300")),
            analyze_every_n_events=int(os.getenv("ANALYZE_EVERY_N_EVENTS", "1")),
            late_event_threshold_seconds=float(
                os.getenv("LATE_EVENT_THRESHOLD_SECONDS", "60")
            ),
            stream_warning_lag_seconds=float(
                os.getenv("STREAM_WARNING_LAG_SECONDS", "60")
            ),
            stream_critical_lag_seconds=float(
                os.getenv("STREAM_CRITICAL_LAG_SECONDS", "300")
            ),
            invalid_event_rate_warning=float(
                os.getenv("INVALID_EVENT_RATE_WARNING", "0.01")
            ),
            invalid_event_rate_critical=float(
                os.getenv("INVALID_EVENT_RATE_CRITICAL", "0.05")
            ),
            producer_interval_seconds=float(
                os.getenv("PRODUCER_INTERVAL_SECONDS", "0.2")
            ),
            producer_seed=int(os.getenv("PRODUCER_SEED", "42")),
            producer_drift_after_events=int(
                os.getenv("PRODUCER_DRIFT_AFTER_EVENTS", "700")
            ),
        )

    def validate(self) -> None:
        """Проверить значения настроек перед запуском сервиса"""
        if not self.kafka_bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must not be empty")
        if not self.kafka_topic.strip():
            raise ValueError("KAFKA_TOPIC must not be empty")
        if not self.kafka_group_id.strip():
            raise ValueError("KAFKA_GROUP_ID must not be empty")
        if self.kafka_auto_offset_reset not in {"earliest", "latest", "error"}:
            raise ValueError(
                "KAFKA_AUTO_OFFSET_RESET must be one of: earliest, latest, error"
            )
        if not 1 <= self.prometheus_port <= 65_535:
            raise ValueError("PROMETHEUS_PORT must be in [1, 65535]")
        if self.window_size <= 0:
            raise ValueError("WINDOW_SIZE must be > 0")
        if not 0 < self.min_window_size <= self.window_size:
            raise ValueError("MIN_WINDOW_SIZE must be in [1, WINDOW_SIZE]")
        if self.analyze_every_n_events <= 0:
            raise ValueError("ANALYZE_EVERY_N_EVENTS must be > 0")
        if self.late_event_threshold_seconds < 0:
            raise ValueError("LATE_EVENT_THRESHOLD_SECONDS must be >= 0")
        if self.stream_warning_lag_seconds < 0:
            raise ValueError("STREAM_WARNING_LAG_SECONDS must be >= 0")
        if self.stream_critical_lag_seconds < self.stream_warning_lag_seconds:
            raise ValueError("STREAM_CRITICAL_LAG_SECONDS must be >= warning threshold")
        if not 0 <= self.invalid_event_rate_warning <= 1:
            raise ValueError("INVALID_EVENT_RATE_WARNING must be in [0, 1]")
        if not 0 <= self.invalid_event_rate_critical <= 1:
            raise ValueError("INVALID_EVENT_RATE_CRITICAL must be in [0, 1]")
        if self.invalid_event_rate_critical < self.invalid_event_rate_warning:
            raise ValueError("INVALID_EVENT_RATE_CRITICAL must be >= warning threshold")
        if self.producer_interval_seconds < 0:
            raise ValueError("PRODUCER_INTERVAL_SECONDS must be >= 0")
        if self.producer_drift_after_events < 0:
            raise ValueError("PRODUCER_DRIFT_AFTER_EVENTS must be >= 0")
