"""Run interval contract shared by orchestrator handoff and runtime.

One thin contract keeps the Airflow (or any scheduler) handoff and the dpone
runtime speaking the same interval language:

- the GitOps Airflow pack templates these environment variables from the
  DAG-run context (``data_interval_start``/``data_interval_end``/...);
- ``dpone run`` reads the same variables (or explicit CLI flags) and exposes
  them to load configs as substitution tokens and run-state identity.

The contract is scheduler-agnostic: any orchestrator that exports the same
environment variables gets identical interval-aware behavior.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

INTERVAL_START_ENV = "DPONE_INTERVAL_START"
INTERVAL_END_ENV = "DPONE_INTERVAL_END"
LOGICAL_DATE_ENV = "DPONE_LOGICAL_DATE"
DAG_ID_ENV = "DPONE_DAG_ID"
DAG_RUN_ID_ENV = "DPONE_DAG_RUN_ID"
TRY_NUMBER_ENV = "DPONE_TRY_NUMBER"
PARTITION_KEY_ENV = "DPONE_PARTITION_KEY"
PARTITION_DIMENSION_ENV = "DPONE_PARTITION_DIMENSION"
PARTITION_MODE_ENV = "DPONE_PARTITION_MODE"

RUN_INTERVAL_ENV_NAMES = (
    DAG_ID_ENV,
    DAG_RUN_ID_ENV,
    TRY_NUMBER_ENV,
    LOGICAL_DATE_ENV,
    INTERVAL_START_ENV,
    INTERVAL_END_ENV,
    PARTITION_KEY_ENV,
    PARTITION_DIMENSION_ENV,
    PARTITION_MODE_ENV,
)

_EMPTY_TOKENS = {"", "none", "null"}
_PARTITION_DIMENSION_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_MAX_PARTITION_KEY_LENGTH = 256


@dataclass(frozen=True, slots=True)
class RunInterval:
    """Logical execution window of one scheduled run."""

    interval_start: str | None = None
    interval_end: str | None = None
    logical_date: str | None = None
    dag_id: str | None = None
    dag_run_id: str | None = None
    try_number: str | None = None
    partition_key: str | None = None
    partition_dimension: str | None = None
    partition_mode: str | None = None

    @property
    def is_empty(self) -> bool:
        return not any(
            (
                self.interval_start,
                self.interval_end,
                self.logical_date,
                self.dag_id,
                self.dag_run_id,
                self.partition_key,
                self.partition_dimension,
                self.partition_mode,
            )
        )

    def execution_datetime(self) -> datetime | None:
        """Parsed logical date (falls back to interval start) for run-state identity."""

        for candidate in (self.logical_date, self.interval_start):
            parsed = parse_interval_datetime(candidate)
            if parsed is not None:
                return parsed
        return None

    def substitution_tokens(self) -> dict[str, str]:
        """Template tokens usable inside manifest predicates/options.

        ``{{ ds }}`` follows the Airflow convention: the logical date rendered
        as ``YYYY-MM-DD``.
        """

        tokens: dict[str, str] = {}
        if self.interval_start:
            tokens["data_interval_start"] = self.interval_start
        if self.interval_end:
            tokens["data_interval_end"] = self.interval_end
        if self.logical_date:
            tokens["logical_date"] = self.logical_date
            parsed = parse_interval_datetime(self.logical_date)
            if parsed is not None:
                tokens["ds"] = parsed.date().isoformat()
        if self.dag_run_id:
            tokens["dag_run_id"] = self.dag_run_id
        if self.partition_key:
            tokens["partition_key"] = self.partition_key
        return tokens

    def to_jsonable(self) -> dict[str, str | None]:
        return {
            "interval_start": self.interval_start,
            "interval_end": self.interval_end,
            "logical_date": self.logical_date,
            "dag_id": self.dag_id,
            "dag_run_id": self.dag_run_id,
            "try_number": self.try_number,
            "partition_key": self.partition_key,
            "partition_dimension": self.partition_dimension,
            "partition_mode": self.partition_mode,
        }


def run_interval_from_env(env: Mapping[str, str] | None = None) -> RunInterval:
    """Build a RunInterval from the process environment (missing values → None)."""

    source = os.environ if env is None else env
    return RunInterval(
        interval_start=_clean(source.get(INTERVAL_START_ENV)),
        interval_end=_clean(source.get(INTERVAL_END_ENV)),
        logical_date=_clean(source.get(LOGICAL_DATE_ENV)),
        dag_id=_clean(source.get(DAG_ID_ENV)),
        dag_run_id=_clean(source.get(DAG_RUN_ID_ENV)),
        try_number=_clean(source.get(TRY_NUMBER_ENV)),
        partition_key=_clean(source.get(PARTITION_KEY_ENV)),
        partition_dimension=_clean(source.get(PARTITION_DIMENSION_ENV)),
        partition_mode=_clean(source.get(PARTITION_MODE_ENV)),
    )


def validate_partition_context(interval: RunInterval) -> None:
    """Fail closed when a native partitioned task lacks its scheduler key."""

    partition_fields = (interval.partition_key, interval.partition_dimension, interval.partition_mode)
    if not any(partition_fields):
        return
    if interval.partition_mode is None:
        raise ValueError("DPONE_AIRFLOW_PARTITION_MODE_MISSING: partition mode is required")
    if interval.partition_mode not in {"native", "degraded_unpartitioned"}:
        raise ValueError(
            f"DPONE_AIRFLOW_PARTITION_MODE_INVALID: unsupported partition mode {interval.partition_mode!r}"
        )
    if not interval.partition_dimension:
        raise ValueError("DPONE_AIRFLOW_PARTITION_DIMENSION_MISSING: partition dimension is required")
    if not _PARTITION_DIMENSION_PATTERN.fullmatch(interval.partition_dimension):
        raise ValueError("DPONE_AIRFLOW_PARTITION_DIMENSION_INVALID: partition dimension is invalid")
    if interval.partition_key and (
        len(interval.partition_key) > _MAX_PARTITION_KEY_LENGTH
        or any(ord(character) < 32 for character in interval.partition_key)
    ):
        raise ValueError("DPONE_AIRFLOW_PARTITION_KEY_INVALID: partition key is invalid")
    if interval.partition_mode == "native" and not interval.partition_key:
        raise ValueError("DPONE_AIRFLOW_PARTITION_KEY_MISSING: native partitioned run requires partition key")


def parse_interval_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-8601 boundary, tolerating the Airflow ``Z`` suffix."""

    text = _clean(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _clean(value: str | None) -> str | None:
    text = str(value or "").strip()
    if text.lower() in _EMPTY_TOKENS:
        return None
    return text


__all__ = [
    "DAG_ID_ENV",
    "DAG_RUN_ID_ENV",
    "INTERVAL_END_ENV",
    "INTERVAL_START_ENV",
    "LOGICAL_DATE_ENV",
    "PARTITION_DIMENSION_ENV",
    "PARTITION_KEY_ENV",
    "PARTITION_MODE_ENV",
    "RUN_INTERVAL_ENV_NAMES",
    "TRY_NUMBER_ENV",
    "RunInterval",
    "parse_interval_datetime",
    "run_interval_from_env",
    "validate_partition_context",
]
