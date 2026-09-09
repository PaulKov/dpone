"""Apply a run interval to load configurations before execution.

Manifest authors can reference the interval anywhere in string options and
predicates using ``{{ token }}`` placeholders:

.. code-block:: yaml

    sink:
      strategy:
        mode: backfill
        backfill:
          inner_mode: partition_replace
          chunk:
            column: business_date
            from: "{{ data_interval_start }}"
            to: "{{ data_interval_end }}"
            step: 1d

Supported tokens: ``data_interval_start``, ``data_interval_end``,
``logical_date``, ``ds`` (logical date as ``YYYY-MM-DD``), ``dag_run_id``.

Values come from CLI flags (``dpone run --interval-start/--interval-end``) or
the ``DPONE_*`` environment contract emitted by the Airflow GitOps pack, so
the same manifest is idempotently re-runnable per interval (functional data
engineering: a re-run of one interval replaces exactly its own slice).
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from dpone.contracts.run_interval import (
    RunInterval,
    parse_interval_datetime,
    run_interval_from_env,
    validate_partition_context,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_TOKEN_PATTERN = re.compile(r"\{\{\s*(?P<name>[a-z_]+)\s*\}\}")


def resolve_run_interval(args: Any) -> RunInterval:
    """Resolve the run interval: CLI flags take precedence over DPONE_* env."""

    interval = run_interval_from_env()
    overrides = {
        "interval_start": getattr(args, "interval_start", None),
        "interval_end": getattr(args, "interval_end", None),
        "logical_date": getattr(args, "execution_date", None),
        "dag_id": getattr(args, "dag_id", None),
    }
    updates = {key: value for key, value in overrides.items() if value}
    return replace(interval, **updates) if updates else interval


def resolve_execution_date(args: Any, interval: RunInterval) -> Any:
    """Run-state execution date: explicit CLI value first, then the interval."""

    explicit = parse_interval_datetime(getattr(args, "execution_date", None))
    if explicit is not None:
        return explicit
    if getattr(args, "execution_date", None):
        return args.execution_date
    return interval.execution_datetime()


class IntervalContextService:
    """Substitute interval tokens and attach interval metadata to a LoadConfig."""

    def __init__(self, interval: RunInterval) -> None:
        validate_partition_context(interval)
        self._interval = interval
        self._tokens = interval.substitution_tokens()

    @property
    def interval(self) -> RunInterval:
        return self._interval

    def apply(self, load_config: LoadConfig) -> LoadConfig:
        """Return the load config with interval tokens resolved in-place.

        No-op when the interval carries no values, so non-scheduled runs stay
        byte-identical to today's behavior.
        """

        if self._interval.is_empty:
            return load_config
        load_config.options = self._substitute(load_config.options or {})
        load_config.options["interval"] = self._interval.to_jsonable()
        if load_config.custom_predicate:
            load_config.custom_predicate = self._substitute(load_config.custom_predicate)
        return load_config

    def _substitute(self, value: Any) -> Any:
        if isinstance(value, str):
            return _TOKEN_PATTERN.sub(self._resolve_token, value)
        if isinstance(value, dict):
            return {key: self._substitute(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._substitute(item) for item in value]
        return value

    def _resolve_token(self, match: re.Match[str]) -> str:
        name = match.group("name")
        resolved = self._tokens.get(name)
        if resolved is None:
            return match.group(0)
        return resolved


__all__ = ["IntervalContextService", "resolve_execution_date", "resolve_run_interval"]
