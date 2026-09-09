"""ClickHouse row-count validation helpers for ETLLogger."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Protocol

import pytz


class ETLLoggerValidationOwner(Protocol):
    metrics: Any

    def _format_table(self, rows: list[dict[str, Any]], title: str | None = None) -> list[str]: ...

    def _log_level(self, name: str) -> Any: ...

    def _log_with_format(self, level: Any, message: str, details: dict[str, Any] | None = None) -> None: ...

    def log_etl_error(self, error: str, context: dict[str, Any] | None = None) -> None: ...


def validate_single_partition(
    owner: ETLLoggerValidationOwner,
    partition_date: str,
    source_connector: Any,
    source_schema: str,
    source_table: str,
    date_column: str,
    partition_by: str,
    target_count: int,
) -> bool:
    """
    Валидирует одну партицию: сравнивает количество строк в ClickHouse с target_count.

    Args:
        partition_date: Дата партиции (YYYY-MM-DD)
        source_connector: ClickHouseConnector для подсчета строк
        source_schema: Схема источника
        source_table: Таблица источника
        date_column: Колонка с датой
        partition_by: Тип партиционирования ('day', 'month', 'year')
        target_count: Количество строк в целевой системе (BigQuery) для этой партиции

    Returns:
        True если совпадает, False если есть расхождение
    """
    try:
        # Подсчитываем строки в ClickHouse для этой партиции
        ch_count = source_connector.count_rows_by_date(
            schema=source_schema,
            table=source_table,
            date_column=date_column,
            partition_date=partition_date,
            partition_by=partition_by,
        )

        match = ch_count == target_count

        # Проверяем, является ли партиция текущей датой (по московскому времени)
        try:
            partition_date_obj = date.fromisoformat(partition_date)
            # Получаем текущую дату в московском времени
            moscow_tz = pytz.timezone("Europe/Moscow")
            current_date_msk = datetime.now(moscow_tz).date()
            is_current_date = partition_date_obj == current_date_msk
        except (ValueError, AttributeError):
            is_current_date = False

        # Если это текущая дата и есть расхождение - это нормально для real-time данных
        if is_current_date and not match:
            status_icon = "ℹ️"
            status_text = "INFO (Real-time data)"
            diff = abs(ch_count - target_count)
            log_level = owner._log_level("INFO")
            is_warning = False
        else:
            status_icon = "✅" if match else "⚠️"
            status_text = "SUCCESS" if match else "WARNING"
            diff = abs(ch_count - target_count)
            log_level = owner._log_level("SUCCESS" if match else "WARNING")
            is_warning = not match

        # Формируем данные для таблицы
        table_rows = [
            {"Metric": "Date", "Value": partition_date},
            {"Metric": "ClickHouse Rows", "Value": f"{ch_count:,}"},
            {"Metric": "Target Rows", "Value": f"{target_count:,}"},
            {"Metric": "Status", "Value": f"{status_icon} {status_text}"},
        ]

        if not match:
            table_rows.append({"Metric": "Difference", "Value": f"{diff:,} rows"})
            if is_current_date:
                table_rows.append({"Metric": "Note", "Value": "ClickHouse real-time data may differ"})

        # Форматируем таблицу
        table_lines = owner._format_table(table_rows, title=f"PARTITION: {partition_date}")

        # Выводим таблицу
        details = {
            "EMPTY_LINE_VALIDATION": "",
            f"🔍 PARTITION VALIDATION: {partition_date}": "",
        }
        for i, line in enumerate(table_lines, 1):
            details[f"TABLE_LINE_{i}"] = line

        owner._log_with_format(log_level, f"PARTITION VALIDATION: {partition_date}", details)

        if is_warning and owner.metrics:
            owner.metrics.warnings_count += 1

        return match

    except Exception as e:
        owner.log_etl_error(
            f"Ошибка при валидации партиции {partition_date}: {str(e)}",
            {
                "partition_date": partition_date,
                "source_schema": source_schema,
                "source_table": source_table,
            },
        )
        return False


def validate_clickhouse_rows(
    owner: ETLLoggerValidationOwner,
    result: dict[str, Any],
    validation_info: dict[str, Any],
    details: dict[str, Any],
) -> None:
    """
    Валидирует количество строк между ClickHouse и BigQuery.

    Для каждой партиции (даты):
    - Подсчитывает строки в ClickHouse для этой даты
    - Сравнивает с Final Count из BQ (если одна дата) или выводит сводную таблицу
    - Выводит статус SUCCESS/WARNING для каждой даты
    """
    try:
        source_connector = validation_info.get("source_connector")
        source_schema = validation_info.get("source_schema")
        source_table = validation_info.get("source_table")
        date_column = validation_info.get("date_column")
        partition_by = validation_info.get("partition_by", "day")
        partitions = validation_info.get("partitions", [])
        final_count = validation_info.get("final_count", 0)

        if not source_connector or not partitions:
            return

        # Вызываем метод count_rows_by_date для каждой партиции и валидируем каждую отдельно
        validation_results = []
        total_ch_rows = 0

        # Получаем сохраненные результаты валидации партиций (если есть)
        partition_validation_results = validation_info.get("partition_validation_results") or {}

        # Для одной даты - сравниваем с final_count напрямую
        # Для нескольких дат - используем сохраненные результаты валидации партиций

        for partition_date in partitions:
            ch_count = source_connector.count_rows_by_date(
                schema=source_schema,
                table=source_table,
                date_column=date_column,
                partition_date=partition_date,
                partition_by=partition_by,
            )
            total_ch_rows += ch_count

            # Проверяем, является ли эта партиция текущей датой
            try:
                partition_date_obj = date.fromisoformat(partition_date)
                moscow_tz = pytz.timezone("Europe/Moscow")
                current_date_msk = datetime.now(moscow_tz).date()
                is_current_date = partition_date_obj == current_date_msk
            except (ValueError, AttributeError):
                is_current_date = False

            # Получаем информацию о валидации для этой партиции (если есть)
            partition_val_info = partition_validation_results.get(partition_date, {})
            target_count = partition_val_info.get("target_count", 0)
            is_match = partition_val_info.get("match", None)

            validation_results.append(
                {
                    "date": partition_date,
                    "ch_count": ch_count,
                    "target_count": target_count,
                    "is_match": is_match,
                    "is_current_date": is_current_date,
                }
            )

        # Если одна партиция - сравниваем с Final Count
        # Если несколько - выводим сводную таблицу по дням
        if len(partitions) == 1:
            # Одна дата - сравниваем Final Count с подсчетом в CH
            partition_date = partitions[0]
            ch_count = validation_results[0]["ch_count"]
            match = ch_count == final_count

            # Проверяем, является ли партиция текущей датой (по московскому времени)
            try:
                partition_date_obj = date.fromisoformat(partition_date)
                # Получаем текущую дату в московском времени
                moscow_tz = pytz.timezone("Europe/Moscow")
                current_date_msk = datetime.now(moscow_tz).date()
                is_current_date = partition_date_obj == current_date_msk
            except (ValueError, AttributeError):
                is_current_date = False

            # Если это текущая дата и есть расхождение - это нормально для real-time данных
            if is_current_date and not match:
                status_icon = "ℹ️"
                status_text = "INFO (Real-time data)"
                diff = abs(ch_count - final_count)
                is_warning = False
            else:
                status_icon = "✅" if match else "⚠️"
                status_text = "SUCCESS" if match else "WARNING"
                diff = abs(ch_count - final_count)
                is_warning = not match

            # Формируем данные для таблицы
            table_rows = [
                {"Metric": "Date", "Value": partition_date},
                {"Metric": "ClickHouse Rows", "Value": f"{ch_count:,}"},
                {"Metric": "BigQuery Final Count", "Value": f"{final_count:,}"},
                {"Metric": "Status", "Value": f"{status_icon} {status_text}"},
            ]

            if not match:
                table_rows.append({"Metric": "Difference", "Value": f"{diff:,} rows"})
                if is_current_date:
                    table_rows.append({"Metric": "Note", "Value": "ClickHouse real-time data may differ"})

            # Форматируем таблицу
            table_lines = owner._format_table(table_rows, title="DATA VALIDATION REPORT")

            # Выводим таблицу
            details = {
                "EMPTY_LINE_VALIDATION": "",
                "🔍 VALIDATION SUMMARY": "",
            }
            for i, line in enumerate(table_lines, 1):
                details[f"TABLE_LINE_{i}"] = line

            # Выводим через _log_with_format
            owner._log_with_format(
                owner._log_level("INFO" if is_current_date and not match else ("SUCCESS" if match else "WARNING")),
                "VALIDATION SUMMARY",
                details,
            )

            if is_warning and owner.metrics:
                owner.metrics.warnings_count += 1
        else:
            # Несколько дат - выводим красивую сводную таблицу по дням
            total_match = total_ch_rows == final_count

            # Проверяем, есть ли текущая дата среди партиций (по московскому времени)
            has_current_date = False
            moscow_tz = pytz.timezone("Europe/Moscow")
            current_date_msk = datetime.now(moscow_tz).date()

            for val_result in validation_results:
                date_str = val_result["date"]
                try:
                    partition_date_obj = date.fromisoformat(date_str)
                    is_current_date = partition_date_obj == current_date_msk
                    if is_current_date:
                        has_current_date = True
                        break  # Достаточно найти одну текущую дату
                except (ValueError, AttributeError):
                    pass

            if not total_match and has_current_date:
                status_icon = "ℹ️"
                status_text = "INFO (Real-time data may differ)"
                diff = abs(total_ch_rows - final_count)
                is_warning_total = False
            else:
                status_icon = "✅" if total_match else "⚠️"
                status_text = "SUCCESS" if total_match else "WARNING"
                diff = abs(total_ch_rows - final_count) if not total_match else 0
                is_warning_total = not total_match

            # Формируем данные для таблицы
            table_rows = []
            moscow_tz = pytz.timezone("Europe/Moscow")
            current_date_msk = datetime.now(moscow_tz).date()

            # Определяем, есть ли вообще расхождения (total mismatch)
            has_total_mismatch = total_ch_rows != final_count

            for val_result in validation_results:
                date_str = val_result["date"]
                ch_count = val_result["ch_count"]
                target_count = val_result.get("target_count", 0)
                is_match = val_result.get("is_match", None)
                is_current_date = val_result["is_current_date"]

                # Определяем статус для строки:
                # 1. Текущая дата - всегда ℹ️ OK (Real-time)
                # 2. Есть результат валидации партиции (is_match) - используем его
                # 3. Нет результата - проверяем общее расхождение (fallback)
                if is_current_date:
                    row_status = "ℹ️ OK (Real-time)"
                elif is_match is not None:
                    # Используем результат валидации конкретной партиции
                    if is_match:
                        row_status = "✅ OK" if ch_count > 0 else "❌ EMPTY"
                    else:
                        # Есть расхождение для этой конкретной партиции
                        row_status = "⚠️ Late-Arriving Rows" if ch_count > 0 else "❌ EMPTY"
                elif has_total_mismatch:
                    # Fallback: если есть общее расхождение, то для не-текущих дат ставим Late-Arriving
                    row_status = "⚠️ Late-Arriving Rows" if ch_count > 0 else "❌ EMPTY"
                else:
                    row_status = "✅ OK" if ch_count > 0 else "❌ EMPTY"

                table_rows.append({"Date": date_str, "ClickHouse Rows": f"{ch_count:,}", "Status": row_status})

            table_rows.append({"Date": "─" * 20, "ClickHouse Rows": "─" * 15, "Status": "─" * 20})

            # Добавляем итоговые строки
            table_rows.append({"Date": "TOTAL", "ClickHouse Rows": f"{total_ch_rows:,}", "Status": ""})
            table_rows.append({"Date": "BigQuery Final Count", "ClickHouse Rows": f"{final_count:,}", "Status": ""})

            table_rows.append({"Date": "─" * 20, "ClickHouse Rows": "─" * 15, "Status": "─" * 20})

            # Добавляем статус и примечания
            table_rows.append(
                {"Date": "Validation Status", "ClickHouse Rows": f"{status_icon} {status_text}", "Status": ""}
            )

            if not total_match:
                table_rows.append({"Date": "Difference", "ClickHouse Rows": f"{diff:,} rows", "Status": ""})
                if has_current_date:
                    table_rows.append({"Date": "Note", "ClickHouse Rows": "Real-time data may differ", "Status": ""})

            # Форматируем таблицу
            table_lines = owner._format_table(table_rows, title="DATA VALIDATION REPORT BY DATE")

            # Выводим таблицу
            details = {
                "EMPTY_LINE_VALIDATION": "",
                "🔍 VALIDATION SUMMARY BY DATE": "",
            }
            for i, line in enumerate(table_lines, 1):
                details[f"TABLE_LINE_{i}"] = line

            # Выводим через _log_with_format
            owner._log_with_format(
                owner._log_level(
                    "INFO" if has_current_date and not total_match else ("SUCCESS" if total_match else "WARNING")
                ),
                "VALIDATION SUMMARY BY DATE",
                details,
            )

            if is_warning_total and owner.metrics:
                owner.metrics.warnings_count += 1

    except Exception as e:
        owner.log_etl_error(
            f"Ошибка при валидации ClickHouse rows: {str(e)}", {"validation_info": str(validation_info)}
        )


__all__ = ["validate_clickhouse_rows", "validate_single_partition"]
