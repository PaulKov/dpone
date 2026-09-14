"""Finite admission policy and stable failures for validated ClickHouse files."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from dpone.runtime.clickhouse_file_stage_contract import (
    ABORT_PHASE_SECONDS as ABORT_PHASE_SECONDS,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    CHUNK_BYTES as CHUNK_BYTES,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    MAX_EVENT_BYTES as MAX_EVENT_BYTES,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    MAX_RESPONSE_BYTES as MAX_RESPONSE_BYTES,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    REMOTE_CONFIRMATION_SECONDS as REMOTE_CONFIRMATION_SECONDS,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    SYNC_SETTINGS,
)


class FileConsumptionError(RuntimeError):
    """An admission/fidelity failure belonging to this explicit staging API."""

    code = "DPONE_CLICKHOUSE_FILE_CONSUMPTION_BLOCKED"

    def __init__(
        self, blocker: str, *, phase: str = "checking", column: str | None = None, row_ordinal: int | None = None
    ) -> None:
        self.blocker = blocker
        self.phase = phase
        self.details: dict[str, Any] = {}
        if column is not None:
            self.details["column"] = column
        if row_ordinal is not None:
            self.details["row_ordinal"] = row_ordinal
        super().__init__(f"{self.code}:{blocker}:{phase}")


@dataclass(frozen=True, slots=True)
class ClickHouseValidatedFilePolicy:
    """Required local storage authority and finite byte/second limits.

    Record bytes include LF. The spool cap includes journal event bytes. The
    source and spool remain separate: source size never authorizes extra disk.
    """

    work_directory: Path
    max_spool_bytes: int
    max_source_bytes: int = 4_294_967_296
    max_record_bytes: int = 16_777_216
    min_free_bytes: int = 1_073_741_824
    preparation_timeout_seconds: int = 3600
    verification_timeout_seconds: int = 3600

    def __post_init__(self) -> None:
        if not isinstance(self.work_directory, Path):
            raise ValueError("work_directory must be a Path")
        for field in fields(self):
            if field.name == "work_directory":
                continue
            value = getattr(self, field.name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{field.name} must be a positive finite integer")


def require_transport_profile(options: Mapping[str, Any]) -> tuple[str, int]:
    """Validate authored values before legacy normalizers can coerce defaults."""
    raw = options.get("clickhouse_bulk", {})
    if not isinstance(raw, Mapping):
        raise FileConsumptionError("settings_unsupported")
    mode = raw.get("mode")
    if mode is None or mode == "auto":
        raise FileConsumptionError("explicit_mode_required")
    modes = {"client": "client", "clickhouse-client": "client", "native_client": "client", "http": "http"}
    if not isinstance(mode, str) or mode not in modes:
        raise FileConsumptionError("transport_unsupported")
    selected = modes[mode]
    selected_options = raw.get(selected, {})
    if not isinstance(selected_options, Mapping):
        raise FileConsumptionError("settings_unsupported")
    timeout = selected_options.get("timeout_seconds", 3600)
    if type(timeout) is not int or timeout <= 0:
        raise FileConsumptionError("resource_limit")
    allowed = {"mode", "client", "http", "input_format", "insert_settings"}
    if set(raw) - allowed:
        raise FileConsumptionError("settings_unsupported")
    if raw.get("input_format", "RowBinary") != "RowBinary":
        raise FileConsumptionError("settings_unsupported")
    connection_keys = {"host", "port", "database", "user", "password", "secure", "timeout_seconds"}
    if selected == "client":
        connection_keys.add("command")
    if set(selected_options) - connection_keys:
        raise FileConsumptionError("settings_unsupported")
    settings = raw.get("insert_settings", {})
    if not isinstance(settings, Mapping) or any(
        key not in SYNC_SETTINGS or type(value) is not int or value != 0 for key, value in settings.items()
    ):
        raise FileConsumptionError("settings_unsupported")
    # Legacy aliases can otherwise smuggle async/settings or an alternate endpoint.
    if any(key.startswith("clickhouse_") and key != "clickhouse_bulk" for key in options):
        raise FileConsumptionError("settings_unsupported")
    physical = options.get("physical_design", {})
    if isinstance(physical, Mapping) and any(physical.get(key) for key in ("cluster", "distributed", "replication")):
        raise FileConsumptionError("settings_unsupported")
    return selected, timeout
