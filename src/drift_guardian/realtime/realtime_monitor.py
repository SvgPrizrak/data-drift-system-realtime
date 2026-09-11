"""Оркестрация sliding window и расчёт временных метрик потока"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from drift_guardian.realtime.engine_adapter import DriftEngine
from drift_guardian.realtime.event import KafkaEvent
from drift_guardian.realtime.window_buffer import WindowBuffer


@dataclass(frozen=True, slots=True)
class StreamSnapshot:
    """Снимок технических метрик текущего состояния потока"""

    status: int
    window_size: int
    event_time_lag_seconds: float
    window_time_span_seconds: float
    max_event_gap_seconds: float
    invalid_event_time_rate: float
    late_events_total: int
    out_of_order_events_total: int


class RealTimeDriftMonitor:
    """Монитор count-based окна и точка вызова drift engine"""

    def __init__(
        self,
        engine: DriftEngine,
        window_size: int,
        min_window_size: int,
        analyze_every_n_events: int = 1,
        late_event_threshold_seconds: float = 60.0,
        stream_warning_lag_seconds: float = 60.0,
        stream_critical_lag_seconds: float = 300.0,
        invalid_event_rate_warning: float = 0.01,
        invalid_event_rate_critical: float = 0.05,
    ) -> None:
        if not 0 < min_window_size <= window_size:
            raise ValueError("min_window_size must be in [1, window_size]")
        if analyze_every_n_events <= 0:
            raise ValueError("analyze_every_n_events must be > 0")
        if late_event_threshold_seconds < 0:
            raise ValueError("late_event_threshold_seconds must be >= 0")
        if stream_warning_lag_seconds < 0:
            raise ValueError("stream_warning_lag_seconds must be >= 0")
        if stream_critical_lag_seconds < stream_warning_lag_seconds:
            raise ValueError("stream_critical_lag_seconds must be >= warning threshold")
        if not 0 <= invalid_event_rate_warning <= 1:
            raise ValueError("invalid_event_rate_warning must be in [0, 1]")
        if not 0 <= invalid_event_rate_critical <= 1:
            raise ValueError("invalid_event_rate_critical must be in [0, 1]")
        if invalid_event_rate_critical < invalid_event_rate_warning:
            raise ValueError("invalid_event_rate_critical must be >= warning threshold")

        self._engine = engine
        self._window = WindowBuffer(window_size)
        self._min_window_size = min_window_size
        self._analyze_every = analyze_every_n_events
        self._late_threshold = late_event_threshold_seconds
        self._warning_lag = stream_warning_lag_seconds
        self._critical_lag = stream_critical_lag_seconds
        self._invalid_warning = invalid_event_rate_warning
        self._invalid_critical = invalid_event_rate_critical

        self._valid_events_total = 0
        self._invalid_event_time_total = 0
        self._late_events_total = 0
        self._out_of_order_events_total = 0
        self._max_event_time_seen: datetime | None = None
        self._latest_lag_seconds = 0.0
        self._events_since_analysis = 0

    def record_invalid_event_time(self) -> None:
        """Учесть запись, в которой не удалось валидировать ``event_time``"""
        self._invalid_event_time_total += 1

    def update(self, event: KafkaEvent) -> dict[str, Any] | None:
        """Добавить событие в окно и при необходимости вернуть новый отчёт"""
        now = datetime.now(UTC)
        event_time = event.event_time.astimezone(UTC)
        lag_seconds = (now - event_time).total_seconds()
        self._latest_lag_seconds = max(lag_seconds, 0.0)

        if self._latest_lag_seconds > self._late_threshold:
            self._late_events_total += 1

        # сравниваем с максимальным event_time, а не только с предыдущим событием
        if (
            self._max_event_time_seen is not None
            and event_time < self._max_event_time_seen
        ):
            self._out_of_order_events_total += 1
        if self._max_event_time_seen is None or event_time > self._max_event_time_seen:
            self._max_event_time_seen = event_time

        self._window.append(event)
        self._valid_events_total += 1
        self._events_since_analysis += 1

        if len(self._window) < self._min_window_size:
            return {
                "overall_status": "insufficient_data",
                "active_alerts": 0,
                "window_size": len(self._window),
                "features": {},
            }

        if self._events_since_analysis < self._analyze_every:
            return None

        self._events_since_analysis = 0
        current_df = self._window.to_dataframe()
        report = self._engine.analyze_dataframe(current_df)
        report.setdefault("window_size", len(self._window))
        return report

    def stream_snapshot(self) -> StreamSnapshot:
        """Рассчитать эксплуатационные метрики потока для Prometheus"""
        event_times = sorted(self._window.event_times())

        if len(event_times) >= 2:
            span = (event_times[-1] - event_times[0]).total_seconds()
            gaps = [
                (right - left).total_seconds()
                for left, right in zip(event_times, event_times[1:], strict=False)
            ]
            max_gap = max(gaps, default=0.0)
        else:
            span = 0.0
            max_gap = 0.0

        timestamp_records = self._valid_events_total + self._invalid_event_time_total
        invalid_rate = (
            self._invalid_event_time_total / timestamp_records
            if timestamp_records
            else 0.0
        )

        status = self._stream_status(invalid_rate)
        return StreamSnapshot(
            status=status,
            window_size=len(self._window),
            event_time_lag_seconds=self._latest_lag_seconds,
            window_time_span_seconds=max(span, 0.0),
            max_event_gap_seconds=max(max_gap, 0.0),
            invalid_event_time_rate=invalid_rate,
            late_events_total=self._late_events_total,
            out_of_order_events_total=self._out_of_order_events_total,
        )

    def _stream_status(self, invalid_rate: float) -> int:
        if len(self._window) < self._min_window_size:
            return -1
        if (
            self._latest_lag_seconds >= self._critical_lag
            or invalid_rate >= self._invalid_critical
        ):
            return 2
        if (
            self._latest_lag_seconds >= self._warning_lag
            or invalid_rate >= self._invalid_warning
        ):
            return 1
        return 0
