"""Контракт Kafka-события"""

from __future__ import annotations

import math
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class KafkaEvent(BaseModel):
    """Событие из топика ``features-stream`` после валидации"""

    model_config = ConfigDict(extra="allow")

    event_id: str | int
    event_time: AwareDatetime
    prediction_score: float | None = Field(default=None, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_extra_features(self) -> KafkaEvent:
        """Проверить, что дополнительные признаки являются скалярами"""
        extras = self.model_extra or {}
        if not extras and self.prediction_score is None:
            raise ValueError("event must contain at least one feature")

        for name, value in extras.items():
            if not isinstance(value, (str, int, float, bool, type(None))):
                raise ValueError(
                    f"feature '{name}' must be a scalar JSON value, "
                    f"got {type(value).__name__}"
                )
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"feature '{name}' must be finite")

        return self

    def feature_payload(self) -> dict[str, Any]:
        """Вернуть признаки без транспортных полей Kafka-события"""
        return self.model_dump(
            mode="python",
            exclude={"event_id", "event_time"},
            exclude_none=False,
        )
