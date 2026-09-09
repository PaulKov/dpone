"""Конфигурация загрузки данных для dpone ETL фреймворка.

Этот модуль содержит классы конфигурации для ETL процессов:
- LoadConfig: конфигурация параметров загрузки
- LoadStrategy: стратегии загрузки данных (enum)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dpone.config.load_strategy import LoadStrategy

ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION = "__dpone_route_identity_v1_endpoint_types"

if TYPE_CHECKING:  # pragma: no cover
    from dpone.config.reconciliation import KeySnapshotReconciliationPolicy
    from dpone.contracts.portable_relation_scope import PortableRelationScope


@dataclass
class LoadConfig:
    """Конфигурация загрузки данных в dpone ETL фреймворке.

    Основные параметры:
    - source/target: подключения, схемы, таблицы
    - load_strategy: стратегия загрузки (из LoadStrategy)
    - unique_key: ключ для merge/dedup операций
    - batch_size: размер батча для обработки
    - log_sample_rows: количество строк для логирования примеров
    - export_format: формат экспорта (csv/binary)
    """

    # === Подключения ===
    source_conn_id: str
    """ID подключения к источнику (Airflow connection или Vault path)"""

    target_conn_id: str
    """ID подключения к целевой системе (Airflow connection или Vault path)"""

    # === Схемы и таблицы источника ===
    source_schema: str
    """Схема источника данных"""

    source_table: str
    """Таблица источника данных"""

    # === Схемы и таблицы приёмника ===
    target_schema: str
    """Целевая схема для загрузки"""

    target_table: str
    """Целевая таблица для загрузки"""

    source_database: str | None = None
    """База данных источника для диалектов с трехчастными именами, например MSSQL."""

    target_database: str | None = None
    """База данных приемника для диалектов с трехчастными именами, например MSSQL."""

    staging_schema: str = "staging"
    """Схема для staging таблиц (по умолчанию: staging)"""

    staging_database: str | None = None
    """База данных staging таблиц; для MSSQL по умолчанию совпадает с target_database."""

    staging_table: str | None = None
    """Имя staging таблицы (генерируется автоматически если не указано)"""

    # === Стратегия загрузки ===
    load_strategy: LoadStrategy = LoadStrategy.FULL_REFRESH
    """Стратегия загрузки данных (по умолчанию: FULL_REFRESH)"""

    unique_key: str | list[str] | None = None
    """Уникальный ключ для merge/dedup операций (строка или список колонок)"""

    only_new_rows: bool = False
    """Для INCREMENTAL_APPEND: True = INSERT NOT EXISTS, False = INSERT ALL (по умолчанию: False)"""

    micro_batch_commit: bool = False
    """Для INCREMENTAL_APPEND: коммит после каждого батча (по умолчанию: False).

    Задаётся в source.options.micro_batch_commit: true

    Преимущества:
    - При падении DAG продолжаем с места остановки (не с начала)
    - Подходит для долгих загрузок с миллионами записей
    - Прогресс виден в target сразу

    Требования:
    - only_new_rows должен быть True (для защиты от дубликатов при перезапуске)
    - unique_key должен быть указан
    """

    overwrite_type: str | None = None
    """Для FULL_REFRESH: способ перезаписи данных.

    Возможные значения:
    - None или 'truncate_insert': TRUNCATE + INSERT (по умолчанию, быстрый режим)
    - 'exchange': Exchange Pattern (атомарная замена через RENAME, zero downtime)

    Exchange Pattern:
    - Создается временная таблица {table}__tmp
    - Текущая таблица переименовывается в {table}__backup
    - {table}__tmp переименовывается в {table}
    - Backup удаляется после успешного swap
    """

    merge_policy: str = "auto"
    """Для INCREMENTAL_MERGE: target-side policy.

    Значение ``auto`` резолвится по sink:
    - MSSQL/Postgres/BigQuery: delete_insert
    - ClickHouse: lightweight_delete_insert
    - Kafka: event_upsert
    """

    duplicate_policy: str = "fail"
    """Для INCREMENTAL_MERGE: поведение при дублях в staging по unique_key.

    v1 поддерживает только fail, чтобы upsert оставался детерминированным.
    """

    allow_non_recommended_policy: bool = False
    """Разрешить explicit non-recommended merge policies, например ClickHouse mutation_delete_insert."""

    mutations_sync: int | None = None
    """ClickHouse mutation/lightweight delete synchronization setting."""

    partition: dict[str, Any] = field(default_factory=dict)
    """Для PARTITION_REPLACE: column, values_from_staging, max_partitions_per_run и dialect hints."""

    # === Дедупликация (опционально) ===
    dedup_expression: str | None = None
    """SQL выражение для дедупликации (например: ROW_NUMBER() OVER ...)"""

    dedup_target: str = "stg"
    """Целевая таблица для дедупликации: 'stg' (staging) или 'tgt' (target)"""

    with_dedup: bool = False
    """Включить дедупликацию (по умолчанию: False)"""

    # === Кастомные предикаты ===
    custom_predicate: str | None = None
    """Кастомный предикат для REPLACE стратегии (например: date = '2025-01-01')"""

    portable_scope: PortableRelationScope | dict[str, Any] | None = None
    """Dialect-neutral equality/range scope for governed cross-database REPLACE.

    Values remain typed DB-API parameters.  Runtime catalog binding proves the
    exact source/target identifier and comparison type before state admission.
    """

    # === Параметры производительности ===
    batch_size: int = 10000
    """Размер батча для обработки данных (по умолчанию: 10000)"""

    log_sample_rows: int = 5
    """Количество строк для логирования примеров данных (по умолчанию: 5)"""

    export_format: str = "csv"
    """Формат экспорта для cross-DB transfer: 'csv' или 'binary' (по умолчанию: csv)"""

    compress_export: bool = False
    """Сжимать экспортируемые файлы gzip (по умолчанию: False).

    Влияние на производительность:
    - True (compress): 25-35 MB/s, файлы в 3-4x меньше
    - False (no compress): 60-100 MB/s, файлы в 3-4x больше (удаляются после загрузки)
    """

    # === Reconciliation (отслеживание удалений) ===
    reconciliation: bool = False
    """Legacy BigQuery-backed reconciliation switch (typed policy does not set it)."""

    reconciliation_policy: KeySnapshotReconciliationPolicy | None = None
    """Typed target-local key-snapshot policy, when configured."""

    repair_authority_ref: str | None = None
    """One-shot environment-owned repair authority selected for this invocation.

    This value is runtime input.  ``LoadConfigBuilder`` intentionally never
    reads it from a promotion manifest.
    """
    tech_schema: str = "tech"
    """Схема для служебных таблиц reconciliation (по умолчанию: tech)"""

    # === Дополнительные параметры (универсальный словарь) ===
    options: dict[str, Any] = field(default_factory=dict)
    """Дополнительные параметры для специфичных коннекторов и стратегий.

    Примеры использования:
    - ClickHouse → GCS export:
        - export_to_gcs: bool
        - gcs_format: str (parquet|csv|orc)
        - gcs_chunk_rows: int
        - date_column: str
        - date_from: str
        - date_to: str
        - partition_by: str (day|month|year)

    - PostgreSQL → BigQuery оптимизация (автоматическая):
        - Таблицы >= 1M строк автоматически загружаются через GCS
        - Порог настраивается через ENV: DPONE_GCS_UPLOAD_THRESHOLD (default: 1000000)
        - Формат: export_format должен быть 'csv'
    """

    source_materialization_work_connection_ref: str | None = None
    """Logical registry ref selected for source snapshot work tables.

    This runtime-owned field is intentionally appended after the established
    constructor parameters so existing positional callers keep their meaning.
    """

    source_materialization_work_database: str | None = None
    """Composition-root-resolved database for source snapshot work tables."""

    source_materialization_work_schema: str | None = None
    """Composition-root-resolved schema for source snapshot work tables."""


__all__ = ["LoadConfig", "LoadStrategy", "ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION"]
