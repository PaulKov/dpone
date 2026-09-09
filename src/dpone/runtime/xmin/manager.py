"""Вычисление безопасного xmin и построение запросов."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone.runtime.state import XMinState
from dpone.runtime.state_logging import etl_logger

if TYPE_CHECKING:
    from dpone.runtime.connectors import PostgresConnector


class XMinStateManager:
    def __init__(self, postgres_connector: PostgresConnector):
        self.postgres_connector = postgres_connector
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_snapshot_xmin_anchor(self) -> int:
        rows = self.postgres_connector.get_records(
            "SELECT txid_snapshot_xmin(txid_current_snapshot())::bigint AS xmin_raw_value",
            as_dict=True,
        )
        if rows:
            value = rows[0]["xmin_raw_value"]
            return int(value) if value is not None else 0
        return 0

    def get_frozen_xid(self) -> int:
        rows = self.postgres_connector.get_records(
            "SELECT datfrozenxid FROM pg_database WHERE datname = current_database()",
            as_dict=True,
        )
        if rows:
            value = rows[0]["datfrozenxid"]
            return int(value) if value is not None else 0
        return 0

    def calculate_safe_xmin(
        self,
        current_xmin: int,
        previous_state: XMinState | None = None,
    ) -> XMinState:
        epoch_mod = 2**32

        def split_xid(full: int) -> tuple[int, int]:
            return full // epoch_mod, full % epoch_mod

        wraparound_detected = False

        if previous_state:
            prev_epoch, prev_low = split_xid(int(previous_state.xmin_value))
            curr_epoch, curr_low = split_xid(current_xmin)
            epoch_diff = curr_epoch - prev_epoch

            wraparound_detected = epoch_diff == 1
            if epoch_diff > 1:
                self.logger.warning(
                    "Detected %s XID wraparounds between syncs. xmin is unsafe — switching to full refresh.",
                    epoch_diff,
                )
                return XMinState(
                    xmin_value=current_xmin,
                    timestamp=datetime.now(),
                    is_initial=True,
                    wraparound_detected=True,
                )

            etl_logger.log_xmin_state_info(
                "XMin окна определены",
                {
                    "Previous": f"epoch={prev_epoch}, low={prev_low}",
                    "Current": f"epoch={curr_epoch}, low={curr_low}",
                },
            )

        frozen_xid = self.get_frozen_xid()
        curr_low = current_xmin % epoch_mod
        frozen_detected = frozen_xid > curr_low if frozen_xid else False

        return XMinState(
            xmin_value=current_xmin,
            timestamp=datetime.now(),
            is_initial=previous_state is None,
            wraparound_detected=wraparound_detected,
            frozen_xid=frozen_xid if frozen_detected else None,
        )

    def build_incremental_query(
        self,
        schema: str,
        table: str,
        prev_state: XMinState | None,
        anchor_full: int,
        columns: list[str] | None = None,
        custom_predicate: str | None = None,
    ) -> str:
        epoch_mod = 2**32

        def split_xid(full: int) -> tuple[int, int]:
            return full // epoch_mod, full % epoch_mod

        columns_str = "*" if not columns else ", ".join(f'"{col}"' for col in columns)
        curr_epoch, curr_low = split_xid(anchor_full)

        if prev_state is None:
            where_condition = "TRUE"
            etl_logger.log_xmin_state_info(
                "Начальный прогон: полный снимок", {"Strategy": "Full snapshot (Airbyte compatible)"}
            )
        else:
            prev_epoch, prev_low = split_xid(int(prev_state.xmin_value))
            diff = curr_epoch - prev_epoch

            if diff == 0:
                etl_logger.log_xmin_state_info(
                    "Обычный случай: инкрементальная загрузка",
                    {
                        "Previous Low": prev_low,
                        "Current Low": curr_low,
                        "Status": "Normal incremental",
                    },
                )
                where_condition = f"(t.xmin::text::bigint >= {prev_low} AND t.xmin::text::bigint < {curr_low})"
            elif diff == 1:
                etl_logger.log_xmin_state_info(
                    "Wraparound между эпохами",
                    {
                        "Previous Epoch": prev_epoch,
                        "Current Epoch": curr_epoch,
                        "Status": "Wraparound detected",
                    },
                )
                where_condition = f"(t.xmin::text::bigint >= {prev_low}) OR (t.xmin::text::bigint < {curr_low})"
            else:
                self.logger.warning(
                    "Detected %s XID wraparounds between syncs. xmin is unsafe — switching to full refresh.",
                    diff,
                )
                where_condition = "TRUE"

        if custom_predicate:
            where_condition = f"({where_condition}) AND ({custom_predicate})"

        query = f"""
        SELECT {columns_str}, t.xmin AS __dpone__xmin
        FROM "{schema}"."{table}" AS t
        WHERE {where_condition}
        """

        etl_logger.log_sql_query(query=query.strip())
        return query

    def get_max_xmin_from_data(self, data: list[dict[str, Any]]) -> int | None:
        if not data:
            return None
        xmin_values = []
        for row in data:
            xmin_val = row.get("__dpone__xmin") if isinstance(row, dict) else None
            if xmin_val is not None:
                xmin_values.append(int(xmin_val))
        if xmin_values:
            max_xmin = max(xmin_values)
            etl_logger.log_xmin_state_info("Максимальный xmin из данных", {"Max XMin": max_xmin})
            return max_xmin
        return None

    @staticmethod
    def should_perform_full_refresh(xmin_state: XMinState) -> bool:
        return xmin_state.is_initial
