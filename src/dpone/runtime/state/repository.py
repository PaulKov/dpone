"""Абстракции для хранилищ состояния ETL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StateKey:
    """Ключ состояния."""

    namespace: str
    name: str


class StateRepository:
    """Интерфейс хранилища состояния."""

    def get_state(self, key: StateKey) -> Any | None:
        """Возвращает состояние по ключу."""

    def save_state(self, key: StateKey, state: Any) -> None:
        """Сохраняет состояние по ключу."""

    def delete_state(self, key: StateKey) -> None:
        """Удаляет состояние."""
