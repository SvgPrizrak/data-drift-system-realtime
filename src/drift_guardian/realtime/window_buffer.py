"""Count-based sliding window для Kafka-событий"""

from __future__ import annotations

from collections import deque
from datetime import datetime

import pandas as pd

from drift_guardian.realtime.event import KafkaEvent


class WindowBuffer:
    """Буфер с последними ``N`` событиями в порядке поступления"""

    def __init__(self, max_size: int) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be > 0")
        self._events: deque[KafkaEvent] = deque(maxlen=max_size)

    def append(self, event: KafkaEvent) -> None:
        """Добавить событие в окно"""
        self._events.append(event)

    def __len__(self) -> int:
        return len(self._events)

    def to_dataframe(self) -> pd.DataFrame:
        """Собрать DataFrame с признаками текущего окна"""
        return pd.DataFrame(event.feature_payload() for event in self._events)

    def event_times(self) -> list[datetime]:
        """Вернуть временные метки событий из текущего окна"""
        return [event.event_time for event in self._events]
