"""Date partition generation and validation helpers for ClickHouseConnector."""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

from dateutil.relativedelta import relativedelta


class ClickHousePartitionService:
    def __init__(self, connector: Any):
        self.connector = connector

    @property
    def logger(self):
        return self.connector.logger

    def _parse_date_value(self, value: str | date | datetime) -> date:
        if isinstance(value, date):
            return value if not isinstance(value, datetime) else value.date()
        if isinstance(value, str):
            value = value[:10]
            return datetime.strptime(value, "%Y-%m-%d").date()
        raise TypeError(f"Unsupported date value type: {type(value)}")

    def generate_date_partitions(
        self,
        date_from: str | date | datetime,
        date_to: str | date | datetime,
        partition_by: str,
        date_column: str,
        current_date: date | None = None,
        last_value: str | None = None,
    ) -> list[tuple[str, str]]:
        date_from_obj = self._parse_date_value(date_from)
        date_to_obj = self._parse_date_value(date_to)

        def _build_predicate(base_predicate: str, is_current_partition: bool) -> str:
            if is_current_partition and current_date and last_value:
                return f"{base_predicate} AND `{date_column}` > '{last_value}'"
            return base_predicate

        partitions = []
        if partition_by == "day":
            current = date_from_obj
            while current <= date_to_obj:
                partition_label = current.strftime("%Y-%m-%d")
                base_predicate = f"toDate(`{date_column}`) = '{partition_label}'"
                is_current = current == current_date
                partitions.append((partition_label, _build_predicate(base_predicate, is_current)))
                current += timedelta(days=1)
        elif partition_by == "month":
            current = date_from_obj.replace(day=1)
            end_month = date_to_obj.replace(day=1)
            while current <= end_month:
                partition_label = current.strftime("%Y-%m-01")
                yyyymm = current.strftime("%Y%m")
                base_predicate = f"toYYYYMM(`{date_column}`) = {yyyymm}"
                is_current = current_date and current.replace(day=1) == current_date.replace(day=1)
                partitions.append((partition_label, _build_predicate(base_predicate, bool(is_current))))
                current += relativedelta(months=1)
        elif partition_by == "year":
            current_year = date_from_obj.year
            end_year = date_to_obj.year
            while current_year <= end_year:
                partition_label = f"{current_year}-01-01"
                base_predicate = f"toYear(`{date_column}`) = {current_year}"
                is_current = current_date and current_year == current_date.year
                partitions.append((partition_label, _build_predicate(base_predicate, bool(is_current))))
                current_year += 1
        else:
            raise ValueError(f"Unsupported partition_by: {partition_by}. Use 'day', 'month', or 'year'.")
        return partitions

    def discover_nonempty_partitions(
        self,
        schema: str,
        table: str,
        date_column: str,
        date_from: str | date | datetime,
        date_to: str | date | datetime,
        partition_by: str,
    ) -> set[str]:
        date_from_obj = self._parse_date_value(date_from)
        date_to_obj = self._parse_date_value(date_to)
        partition_function_map = {
            "day": "toDate",
            "month": "toStartOfMonth",
            "year": "toStartOfYear",
        }
        if partition_by not in partition_function_map:
            raise ValueError(f"Unsupported partition_by: {partition_by}. Use 'day', 'month', or 'year'.")
        ch_function = partition_function_map[partition_by]
        query = f"""
            SELECT {ch_function}(`{date_column}`) as partition_date
            FROM `{schema}`.`{table}`
            WHERE toDate(`{date_column}`) >= toDate(%(date_from)s)
              AND toDate(`{date_column}`) <= toDate(%(date_to)s)
            GROUP BY partition_date
            ORDER BY partition_date
            LIMIT 1 BY partition_date
        """
        params = {
            "date_from": date_from_obj.strftime("%Y-%m-%d"),
            "date_to": date_to_obj.strftime("%Y-%m-%d"),
        }
        if self.logger:
            self.logger.log_etl_progress(
                "CH_PARTITION_DISCOVERY",
                {
                    "Query": query.strip(),
                    "DateFrom": params["date_from"],
                    "DateTo": params["date_to"],
                    "PartitionBy": partition_by,
                },
            )
        start_time = time.time()
        rows = self.connector.get_records(query, params=params, as_dict=True)
        elapsed = time.time() - start_time
        nonempty_partitions = set()
        for row in rows:
            partition_date = row["partition_date"]
            if isinstance(partition_date, str):
                partition_date_obj = self._parse_date_value(partition_date)
            elif isinstance(partition_date, datetime):
                partition_date_obj = partition_date.date()
            elif isinstance(partition_date, date):
                partition_date_obj = partition_date
            else:
                raise ValueError(f"Unexpected partition_date type: {type(partition_date)}")
            if partition_by == "day":
                partition_label = partition_date_obj.strftime("%Y-%m-%d")
            elif partition_by == "month":
                partition_label = partition_date_obj.replace(day=1).strftime("%Y-%m-%d")
            else:
                partition_label = f"{partition_date_obj.year}-01-01"
            nonempty_partitions.add(partition_label)
            if self.logger:
                self.logger.log_etl_progress("CH_PARTITION_FOUND", {"Partition": partition_label})
        if self.logger:
            self.logger.log_etl_progress(
                "CH_PARTITION_DISCOVERY_COMPLETE",
                {
                    "Duration": f"{elapsed:.2f}s",
                    "TotalNonEmptyPartitions": len(nonempty_partitions),
                    "Partitions": sorted(nonempty_partitions),
                },
            )
        return nonempty_partitions

    def count_rows_by_date(
        self,
        schema: str,
        table: str,
        date_column: str,
        partition_date: str,
        partition_by: str = "day",
    ) -> int:
        if partition_by == "day":
            predicate = f"toDate(`{date_column}`) = '{partition_date}'"
        elif partition_by == "month":
            yyyymm = partition_date.replace("-", "")[:6]
            predicate = f"toYYYYMM(`{date_column}`) = {yyyymm}"
        elif partition_by == "year":
            year = partition_date.split("-")[0] if "-" in partition_date else partition_date
            predicate = f"toYear(`{date_column}`) = {year}"
        else:
            raise ValueError(f"Unsupported partition_by: {partition_by}")
        query = f"""
            SELECT COUNT(*) as cnt
            FROM `{schema}`.`{table}`
            WHERE {predicate}
        """
        try:
            result = self.connector.get_records(query, as_dict=True)
            if result and len(result) > 0:
                return int(result[0].get("cnt", 0))
            return 0
        except Exception as e:
            self.logger.log_etl_error(
                f"Ошибка при подсчете строк в ClickHouse для даты {partition_date}: {str(e)}",
                {
                    "schema": schema,
                    "table": table,
                    "date_column": date_column,
                    "partition_date": partition_date,
                },
            )
            return 0
