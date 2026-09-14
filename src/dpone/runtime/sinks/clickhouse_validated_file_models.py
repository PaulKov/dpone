"""Compatibility aliases for the canonical validated-file stage contract."""

from pathlib import Path as Path
from typing import Any as Any

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
    SYNC_SETTINGS as SYNC_SETTINGS,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    ClickHouseValidatedFilePolicy as ClickHouseValidatedFilePolicy,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    FileConsumptionError as FileConsumptionError,
)
from dpone.runtime.clickhouse_file_stage_contract import (
    require_transport_profile as require_transport_profile,
)
