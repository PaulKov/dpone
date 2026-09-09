"""
Стратегия для справочников Omnidesk API (groups, staff, labels).

Особенности:
- Без временных фильтров — выгружает ВСЁ каждый раз
- Поддерживает column_mapping для переименования колонок API → DB
"""

from __future__ import annotations

from typing import Any

from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.api.base import APIBaseStrategy


class OmnideskDictionaryExtractStrategy(APIBaseStrategy):
    """
    Стратегия выгрузки справочников Omnidesk.

    Справочники:
    - groups: Группы (подразделения)
    - staff: Сотрудники
    - labels: Метки

    Особенности:
    - API не поддерживает временные фильтры для справочников
    - Выгружает ВСЕ записи каждый раз
    - Рекомендуется full_refresh
    - Поддержка column_mapping для переименования колонок

    Параметры в options:
    - resource: тип ресурса (groups, staff, labels)
    - exclude_columns: колонки для исключения
    - column_mapping: переименование колонок {api_column: db_column}

    """

    # Маппинг resource → (метод коннектора, ключ элемента)
    RESOURCE_MAP = {
        "groups": ("get_groups", "group"),
        "staff": ("get_staff", "staff"),
        "labels": ("get_labels", "label"),
    }

    # Дефолтные исключаемые колонки для каждого типа
    DEFAULT_EXCLUDE_COLUMNS: dict[str, list[str]] = {
        "groups": [],
        "staff": [],
        "labels": [],
    }

    # Дефолтный маппинг колонок для каждого типа справочника
    # {resource: {api_column: db_column}}
    DEFAULT_COLUMN_MAPPING: dict[str, dict[str, str]] = {
        "groups": {
            "title": "group_title",
            "from_name": "group_from_name",
            "signature": "group_signature",
        },
        "staff": {},
        "labels": {},
    }

    def extract(
        self,
        load_config: Any,
        last_state: dict[str, Any] | None,
    ) -> ExtractResult:
        """
        Извлекает все записи справочника из API.

        Args:
            load_config: Конфигурация загрузки
            last_state: Игнорируется (справочники грузятся целиком)

        Returns:
            ExtractResult с данными справочника
        """
        options = self._get_options(load_config)
        resource = options.get("resource", "groups")
        exclude_columns = set(options.get("exclude_columns", []))

        # Маппинг колонок: пользовательский + дефолтный
        column_mapping = dict(self.DEFAULT_COLUMN_MAPPING.get(resource, {}))
        user_mapping = options.get("column_mapping", {})
        if user_mapping:
            column_mapping.update(user_mapping)

        # Добавляем дефолтные исключения для ресурса
        exclude_columns.update(self.DEFAULT_EXCLUDE_COLUMNS.get(resource, []))

        # Валидируем resource
        if resource not in self.RESOURCE_MAP:
            raise ValueError(
                f"Неизвестный тип справочника: {resource}. Поддерживаемые: {list(self.RESOURCE_MAP.keys())}"
            )

        method_name, _ = self.RESOURCE_MAP[resource]

        self.logger.log_etl_progress(
            "API_DICTIONARY_EXTRACT",
            {
                "Resource": resource,
                "Method": method_name,
                "Exclude_Columns": list(exclude_columns),
                "Column_Mapping": column_mapping,
            },
        )

        # Получаем метод коннектора
        get_method = getattr(self.connector, method_name)

        # Выгружаем все записи
        records: list[dict[str, Any]] = []

        for item in get_method():
            # Фильтруем исключаемые колонки
            if exclude_columns:
                filtered = {k: v for k, v in item.items() if k not in exclude_columns}
            else:
                filtered = dict(item)

            # Применяем маппинг колонок (переименование)
            if column_mapping:
                mapped = {}
                for key, value in filtered.items():
                    new_key = column_mapping.get(key, key)
                    mapped[new_key] = value
                records.append(mapped)
            else:
                records.append(filtered)

        self.logger.info(f"📋 Справочник {resource}: выгружено {len(records)} записей")

        if not records:
            self.logger.warning(f"Справочник {resource} пуст.")
            return ExtractResult(
                artifact=InMemoryRowsArtifact([]),
                schema=[],
                state=None,
                force_full_refresh=True,
            )

        # Определяем схему по первым записям
        schema = self._detect_schema_from_records(records[:10])

        self.logger.info(f"📊 Схема {resource}: {len(schema)} колонок — {', '.join(c[0] for c in list(schema)[:5])}...")

        return ExtractResult(
            artifact=InMemoryRowsArtifact(records),
            schema=schema,
            state=None,
            force_full_refresh=True,
        )
