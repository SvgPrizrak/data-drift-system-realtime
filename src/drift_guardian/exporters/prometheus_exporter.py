"""Экспорт ``drift_report`` и состояния потока в метрики Prometheus"""

from __future__ import annotations

import math
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    disable_created_metrics,
    start_http_server,
)

from drift_guardian.realtime.realtime_monitor import StreamSnapshot

disable_created_metrics()

STATUS_TO_NUMBER = {
    "insufficient_data": -1,
    "ok": 0,
    "warning": 1,
    "critical": 2,
}


class PrometheusExporter:
    """Экспорт drift-отчёта и технических метрик в Prometheus"""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()

        self.overall_status = Gauge(
            "drift_overall_status",
            "Overall drift status: -1 insufficient, 0 ok, 1 warning, 2 critical.",
            registry=self.registry,
        )
        self.active_alerts = Gauge(
            "drift_active_alerts",
            "Number of currently active drift alerts.",
            registry=self.registry,
        )
        self.window_size = Gauge(
            "drift_window_size",
            "Current number of events in the sliding window.",
            registry=self.registry,
        )
        self.events_processed = Counter(
            "drift_events_processed_total",
            "Validated Kafka events successfully processed by the monitor.",
            registry=self.registry,
        )
        self.invalid_events = Counter(
            "drift_invalid_events_total",
            "Kafka records rejected by JSON or event-schema validation.",
            ["reason"],
            registry=self.registry,
        )
        self.processing_errors = Counter(
            "drift_processing_errors_total",
            "Fatal errors while processing otherwise valid Kafka events.",
            ["stage"],
            registry=self.registry,
        )

        feature_labels = ["feature", "type"]
        self.feature_status = Gauge(
            "drift_feature_status",
            "Per-feature status: -1 insufficient, 0 ok, 1 warning, 2 critical.",
            feature_labels,
            registry=self.registry,
        )
        self.feature_psi = Gauge(
            "drift_feature_psi",
            "Population Stability Index for a feature.",
            feature_labels,
            registry=self.registry,
        )
        self.feature_missing_rate = Gauge(
            "drift_feature_missing_rate",
            "Missing-value rate for a feature.",
            feature_labels,
            registry=self.registry,
        )
        self.feature_mean_zscore = Gauge(
            "drift_feature_mean_zscore",
            "Mean z-score shift for a numeric feature.",
            feature_labels,
            registry=self.registry,
        )
        self.feature_unseen_category_rate = Gauge(
            "drift_feature_unseen_category_rate",
            "Rate of categories unseen in the reference data.",
            feature_labels,
            registry=self.registry,
        )
        self.feature_cardinality_ratio = Gauge(
            "drift_feature_cardinality_ratio",
            "Current/reference categorical cardinality ratio.",
            feature_labels,
            registry=self.registry,
        )

        self.prediction_status = Gauge(
            "drift_prediction_status",
            "Prediction drift status.",
            registry=self.registry,
        )
        self.prediction_psi = Gauge(
            "drift_prediction_psi",
            "PSI for model prediction scores.",
            registry=self.registry,
        )
        self.prediction_positive_rate = Gauge(
            "drift_prediction_positive_rate",
            "Positive prediction rate in the current window.",
            registry=self.registry,
        )

        self.stream_status = Gauge(
            "drift_stream_status",
            "Stream status: -1 insufficient, 0 ok, 1 warning, 2 critical.",
            registry=self.registry,
        )
        self.event_time_lag_seconds = Gauge(
            "drift_event_time_lag_seconds",
            "Processing time minus latest valid event_time in seconds.",
            registry=self.registry,
        )
        self.window_time_span_seconds = Gauge(
            "drift_window_time_span_seconds",
            "Time span between oldest and newest timestamps in the window.",
            registry=self.registry,
        )
        self.max_event_gap_seconds = Gauge(
            "drift_max_event_gap_seconds",
            "Largest chronological timestamp gap inside the current window.",
            registry=self.registry,
        )
        self.invalid_event_time_rate = Gauge(
            "drift_invalid_event_time_rate",
            "Share of records rejected due to invalid event_time.",
            registry=self.registry,
        )
        self.late_events = Counter(
            "drift_late_events_total",
            "Valid events whose processing lag exceeded the configured threshold.",
            registry=self.registry,
        )
        self.out_of_order_events = Counter(
            "drift_out_of_order_events_total",
            "Valid events arriving older than the maximum observed event_time.",
            registry=self.registry,
        )

        self._feature_metric_gauges = {
            "psi": self.feature_psi,
            "missing_rate": self.feature_missing_rate,
            "mean_zscore": self.feature_mean_zscore,
            "unseen_category_rate": self.feature_unseen_category_rate,
            "cardinality_ratio": self.feature_cardinality_ratio,
        }
        self._active_feature_labels: dict[str, set[tuple[str, str]]] = {
            name: set() for name in self._feature_metric_gauges
        }
        self._active_status_labels: set[tuple[str, str]] = set()
        self._last_late_total = 0
        self._last_out_of_order_total = 0

    def start_http_server(self, port: int) -> tuple[Any, Any]:
        """Запустить HTTP endpoint ``/metrics`` для текущего registry"""
        return start_http_server(port, registry=self.registry)

    def record_processed_event(self) -> None:
        """Увеличить счётчик успешно обработанных событий"""
        self.events_processed.inc()

    def record_invalid_event(self, reason: str) -> None:
        """Увеличить счётчик отклонённых событий с указанной причиной"""
        self.invalid_events.labels(reason=reason).inc()

    def record_processing_error(self, stage: str) -> None:
        """Увеличить счётчик фатальных ошибок этапа обработки"""
        self.processing_errors.labels(stage=stage).inc()

    def update_report(self, report: dict[str, Any]) -> None:
        """Перенести значения из ``drift_report`` в Prometheus gauges"""
        self.overall_status.set(self._status_value(report.get("overall_status")))
        self.active_alerts.set(float(report.get("active_alerts", 0)))
        self.window_size.set(float(report.get("window_size", 0)))

        features = report.get("features") or {}
        self._update_features(features)
        self._update_prediction(report.get("prediction"))

    def update_stream(self, snapshot: StreamSnapshot) -> None:
        """Обновить метрики, принадлежащие потоковому слою"""
        self.stream_status.set(snapshot.status)
        self.window_size.set(snapshot.window_size)
        self.event_time_lag_seconds.set(snapshot.event_time_lag_seconds)
        self.window_time_span_seconds.set(snapshot.window_time_span_seconds)
        self.max_event_gap_seconds.set(snapshot.max_event_gap_seconds)
        self.invalid_event_time_rate.set(snapshot.invalid_event_time_rate)

        late_delta = snapshot.late_events_total - self._last_late_total
        if late_delta > 0:
            self.late_events.inc(late_delta)
        self._last_late_total = snapshot.late_events_total

        order_delta = snapshot.out_of_order_events_total - self._last_out_of_order_total
        if order_delta > 0:
            self.out_of_order_events.inc(order_delta)
        self._last_out_of_order_total = snapshot.out_of_order_events_total

    def _update_features(self, features: dict[str, Any]) -> None:
        current_status_labels: set[tuple[str, str]] = set()
        current_metric_labels: dict[str, set[tuple[str, str]]] = {
            name: set() for name in self._feature_metric_gauges
        }

        for feature_name, payload in features.items():
            feature_type = str(payload.get("type", "unknown"))
            labels = (str(feature_name), feature_type)
            current_status_labels.add(labels)
            self.feature_status.labels(
                feature=labels[0],
                type=labels[1],
            ).set(self._status_value(payload.get("status")))

            metrics = payload.get("metrics") or {}
            for metric_name, gauge in self._feature_metric_gauges.items():
                if metric_name not in metrics:
                    continue
                value = metrics[metric_name]
                if value is None:
                    continue
                gauge.labels(
                    feature=labels[0],
                    type=labels[1],
                ).set(float(value))
                current_metric_labels[metric_name].add(labels)

        # удаляем series для фичей, исчезнувших из нового отчёта
        for labels in self._active_status_labels - current_status_labels:
            self.feature_status.remove(*labels)
        self._active_status_labels = current_status_labels

        for metric_name, gauge in self._feature_metric_gauges.items():
            stale = (
                self._active_feature_labels[metric_name]
                - current_metric_labels[metric_name]
            )
            for labels in stale:
                gauge.remove(*labels)
            self._active_feature_labels[metric_name] = current_metric_labels[
                metric_name
            ]

    def _update_prediction(self, prediction: dict[str, Any] | None) -> None:
        if not prediction:
            self.prediction_status.set(math.nan)
            self.prediction_psi.set(math.nan)
            self.prediction_positive_rate.set(math.nan)
            return

        self.prediction_status.set(self._status_value(prediction.get("status")))
        metrics = prediction.get("metrics") or {}
        self.prediction_psi.set(self._float_or_nan(metrics.get("prediction_psi")))
        self.prediction_positive_rate.set(
            self._float_or_nan(metrics.get("positive_prediction_rate"))
        )

    @staticmethod
    def _status_value(value: Any) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        return float(STATUS_TO_NUMBER.get(str(value), -1))

    @staticmethod
    def _float_or_nan(value: Any) -> float:
        return math.nan if value is None else float(value)
