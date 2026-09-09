"""ETL logging models and terminal color constants."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


class Colors:
    """ANSI цветовые коды для терминала"""

    RESET = "\033[0m"
    BOLD = "\033[1m"

    FULL_REFRESH = "\033[31m"
    INCREMENTAL = "\033[32m"
    REPLACE = "\033[33m"

    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"


class LogLevel(Enum):
    """Уровни логирования с цветовым кодированием"""

    DEBUG = "🔍"
    INFO = "ℹ️"
    WARNING = "⚠️"
    ERROR = "❌"
    SUCCESS = "✅"
    PERFORMANCE = "⚡"
    DATA = "📊"
    CONNECTION = "🔌"
    BATCH = "🔄"
    STAGE = "🎯"


@dataclass
class ETLMetrics:
    """Метрики ETL процесса"""

    start_time: datetime
    end_time: datetime | None = None
    extracted_rows: int = 0
    staging_rows: int = 0  # ← Новая метрика
    loaded_rows: int = 0
    final_rows: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    soft_deleted_rows: int = 0  # ← Новая метрика
    replaced_rows: int = 0  # ← Новая метрика
    deleted_lookback_rows: int = 0  # ← Новая метрика
    batch_count: int = 0
    error_count: int = 0
    warnings_count: int = 0

    @property
    def duration(self) -> timedelta | None:
        if self.end_time:
            return self.end_time - self.start_time
        return None

    @property
    def rows_per_second(self) -> float | None:
        if self.duration and self.duration.total_seconds() > 0:
            return self.loaded_rows / self.duration.total_seconds()
        return None


__all__ = ["Colors", "ETLMetrics", "LogLevel"]
