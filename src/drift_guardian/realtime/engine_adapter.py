"""Связь realtime-слоя с движком расчёта drift-метрик"""

from __future__ import annotations

from typing import Any, Protocol

import pandas as pd


class DriftEngine(Protocol):
    """Минимальный интерфейс движка, который нужен realtime-слою"""

    def analyze_dataframe(self, current_df: pd.DataFrame) -> dict[str, Any]:
        """Проанализировать текущее окно и вернуть drift report"""
        ...


class MockDriftEngine:
    """Простой движок-заглушка для проверки realtime pipeline"""

    def analyze_dataframe(self, current_df: pd.DataFrame) -> dict[str, Any]:
        """Сформировать тестовый отчёт без расчёта настоящего drift"""
        features: dict[str, Any] = {}

        for column in current_df.columns:
            if column == "prediction_score":
                continue

            series = current_df[column]
            feature_type = (
                "numeric" if pd.api.types.is_numeric_dtype(series) else "categorical"
            )
            features[str(column)] = {
                "type": feature_type,
                "status": "ok",
                "metrics": {
                    "missing_rate": float(series.isna().mean()),
                },
                "alerts": [],
            }

        report: dict[str, Any] = {
            "overall_status": "ok",
            "active_alerts": 0,
            "window_size": len(current_df),
            "features": features,
        }

        if "prediction_score" in current_df.columns:
            prediction = pd.to_numeric(
                current_df["prediction_score"],
                errors="coerce",
            )
            valid_prediction = prediction.dropna()
            positive_rate = (
                float((valid_prediction >= 0.5).mean())
                if not valid_prediction.empty
                else 0.0
            )
            report["prediction"] = {
                "status": "ok",
                "metrics": {
                    "prediction_psi": 0.0,
                    "positive_prediction_rate": positive_rate,
                },
                "alerts": [],
            }

        return report


def build_engine() -> DriftEngine:
    """Вернуть движок, который используется realtime monitor"""
    # после мержа здесь будет создаваться DriftMetricsEngine из первой части
    return MockDriftEngine()
