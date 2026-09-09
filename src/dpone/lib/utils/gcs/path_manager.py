"""GCS path management utilities."""

from __future__ import annotations

from datetime import date, datetime


def parse_date_value(value: date | datetime | str) -> date:
    """
    Конвертирует различные форматы в date объект.
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    elif isinstance(value, datetime):
        return value.date()
    elif isinstance(value, str):
        if "T" in value or " " in value:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.date()
        else:
            dt = datetime.strptime(value, "%Y-%m-%d")
            return dt.date()
    else:
        raise ValueError(f"Cannot parse date from value: {value} (type: {type(value)})")


def format_partition_label(date_obj: date, partition_by: str) -> str:
    """
    Форматирует date в partition label (YYYY-MM-DD format).

    Этот метод генерирует нормализованные лейблы партиций в формате полной даты,
    которые используются в ClickHouse connector и других местах для идентификации партиций.

    Args:
        date_obj: Дата для форматирования
        partition_by: Тип партиционирования ('day', 'month', 'year')

    Returns:
        Partition label в формате:
        - 'day': 'YYYY-MM-DD' (например, '2025-11-16')
        - 'month': 'YYYY-MM-01' (например, '2025-11-01')
        - 'year': 'YYYY-01-01' (например, '2025-01-01')
    """
    if partition_by == "day":
        return f"{date_obj.year:04d}-{date_obj.month:02d}-{date_obj.day:02d}"
    elif partition_by == "month":
        return f"{date_obj.year:04d}-{date_obj.month:02d}-01"
    elif partition_by == "year":
        return f"{date_obj.year:04d}-01-01"
    else:
        # По умолчанию - day
        return f"{date_obj.year:04d}-{date_obj.month:02d}-{date_obj.day:02d}"


def _format_partition_string(date_obj: date, partition_by: str) -> str:
    """
    Форматирует date в Hive-style partition string (dt_date=YYYY-MM-DD).

    Используется внутри модуля для унификации форматирования партиций для GCS paths.
    Использует format_partition_label для генерации normalized date part.

    Args:
        date_obj: Дата для форматирования
        partition_by: Тип партиционирования ('day', 'month', 'year')

    Returns:
        Hive partition string в формате:
        - 'day': 'dt_date=YYYY-MM-DD' (например, 'dt_date=2025-11-16')
        - 'month': 'dt_date=YYYY-MM-01' (например, 'dt_date=2025-11-01')
        - 'year': 'dt_date=YYYY-01-01' (например, 'dt_date=2025-01-01')
    """
    partition_label = format_partition_label(date_obj, partition_by)
    return f"dt_date={partition_label}"


def get_gcs_bucket_name(schema: str, env_code: str | None = None) -> str:
    """Reject implicit bucket inference; callers must configure object storage explicitly."""
    from dpone.runtime.errors import raise_legacy_runtime_defaults_disabled

    del schema, env_code
    raise_legacy_runtime_defaults_disabled(
        detail="Explicit object_storage bucket configuration is required.",
    )


def get_gcs_table_path(
    database: str,
    table: str,
    partition_date: date | datetime | str | None = None,
    partition_by: str | None = None,
    target_schema: str | None = None,
) -> str:
    """
    Генерирует путь внутри GCS бакета в формате Hive-style partitioning для BigQuery.

    Args:
        database: Source database/schema
        table: Source table
        partition_date: Дата партиции (опционально)
        partition_by: Тип партиционирования (day/month/year)
        target_schema: Target schema для префикса пути (опционально, если задан, добавляется в начало)

    Returns:
        Путь в формате:
        - Без target_schema: transfer/{database}/{table}[/dt_date=YYYY-MM-DD]
        - С target_schema: {target_schema}/transfer/{database}/{table}[/dt_date=YYYY-MM-DD]
    """
    if target_schema:
        base_path = f"{target_schema}/transfer/{database}/{table}"
    else:
        base_path = f"transfer/{database}/{table}"

    if not partition_date:
        return base_path

    # Используем приватные helper функции для DRY
    date_obj = parse_date_value(partition_date)
    partition_str = _format_partition_string(date_obj, partition_by or "day")

    return f"{base_path}/{partition_str}"


def format_partition_date(
    date_value: date | datetime | str,
    partition_by: str = "day",
) -> str:
    """
    Форматирует дату для Hive-style партиции (dt_date=YYYY-MM-DD).

    Args:
        date_value: Дата для форматирования
        partition_by: Тип партиционирования ('day' | 'month' | 'year')
    """
    date_obj = parse_date_value(date_value)
    return _format_partition_string(date_obj, partition_by)


def parse_partition_date(partition_str: str) -> date | None:
    """
    Парсит строку Hive-style партиции обратно в date объект.

    Args:
        partition_str: Строка партиции (например, 'dt_date=2025-10-01')
    """
    # Поддержка Hive-style (dt_date=YYYY-MM-DD)
    if not partition_str.startswith("dt_date="):
        return None

    date_part = partition_str.replace("dt_date=", "")
    parts = date_part.split("-")

    try:
        if len(parts) == 3:  # day: YYYY-MM-DD
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
        elif len(parts) == 2:  # month: YYYY-MM
            return date(int(parts[0]), int(parts[1]), 1)
        elif len(parts) == 1:  # year: YYYY
            return date(int(parts[0]), 1, 1)
    except (ValueError, IndexError):
        return None

    return None


def build_gcs_file_pattern(
    file_prefix: str,
    format: str,
    chunk_rows: int | None,
    partitioned: bool = False,
) -> str:
    """
    Строит wildcard pattern для GCS файлов (для BigQuery LOAD).

    Args:
        file_prefix: Префикс имени файла (например, 'data')
        format: Формат файла (например, 'parquet')
        chunk_rows: Если задано, файлы будут с индексом (_0, _1, ...)
        partitioned: Если True, добавляет префикс dt_date=* для Hive partitioning
    """
    if chunk_rows:
        base_pattern = f"{file_prefix}_*.{format}"
    else:
        base_pattern = f"{file_prefix}.{format}"

    if partitioned:
        return f"dt_date=*/{base_pattern}"
    else:
        return base_pattern
