"""Shared init option ownership and argparse actions; no target construction."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

TRACKED_OPTIONS_ATTR = "_init_options_before_target"

_PROJECT_TARGET = frozenset({"project"})
_DOMAIN_TARGET = frozenset({"domain"})
_PIPELINE_TARGET = frozenset({"pipeline"})
_DAG_TARGET = frozenset({"dag"})
_AIRFLOW_TARGETS = _PROJECT_TARGET | _PIPELINE_TARGET
_LEGACY_VALUE_OPTIONS = (
    "--source-type",
    "--sink-type",
    "--source-connection",
    "--sink-connection",
    "--source-schema",
    "--source-table",
    "--target-schema",
    "--target-table",
    "--unique-key",
    "--out",
)
_VALID_TARGETS_BY_OPTION = {
    **{option: frozenset() for option in (*_LEGACY_VALUE_OPTIONS, "--strategy")},
    "--airflow": _AIRFLOW_TARGETS,
    "--no-airflow": _PIPELINE_TARGET,
    "--recipe": _PIPELINE_TARGET,
    "--route": _PIPELINE_TARGET,
    "--profile": _PIPELINE_TARGET,
    "--answers": _PIPELINE_TARGET,
    "--authoring": _PIPELINE_TARGET,
    "--layout": _PROJECT_TARGET,
    "--owner-team": _DOMAIN_TARGET,
    "--owner-contact": _DOMAIN_TARGET,
    "--approver-team": _DOMAIN_TARGET,
    "--domain": _PIPELINE_TARGET | _DAG_TARGET,
    "--from": _PIPELINE_TARGET,
    "--to": _PIPELINE_TARGET,
    "--key": _PIPELINE_TARGET,
    "--schedule": _DAG_TARGET,
    "--pipeline": _DAG_TARGET,
    "--description": _DAG_TARGET,
}


class _TrackedInitOption(argparse.Action):
    """Record options parsed before an optional beginner target."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        value = self.const if self.nargs == 0 else values
        setattr(namespace, self.dest, value)
        tracked = getattr(namespace, TRACKED_OPTIONS_ATTR, ())
        option = option_string or f"--{self.dest.replace('_', '-')}"
        setattr(namespace, TRACKED_OPTIONS_ATTR, (*tracked, (option, _VALID_TARGETS_BY_OPTION[option])))


class _InitTargetParsersAction(argparse._SubParsersAction):
    """Reject options from another init surface before selecting a target."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,
    ) -> None:
        if values is None or isinstance(values, str) or not values:
            parser.error("missing init target")
        target = str(values[0])
        tracked = getattr(namespace, TRACKED_OPTIONS_ATTR, ())
        invalid = tuple(dict.fromkeys(option for option, valid_targets in tracked if target not in valid_targets))
        if invalid:
            target_parser = self._name_parser_map.get(target, parser)
            target_parser.error(f"{target} does not accept: {', '.join(invalid)}")
        super().__call__(parser, namespace, values, option_string)


class _RejectInitOption(argparse.Action):
    """Emit the same target-scoped error regardless of option ordering."""

    def __init__(
        self,
        option_strings: Sequence[str],
        dest: str,
        *,
        init_target: str,
        **kwargs: Any,
    ) -> None:
        self._init_target = init_target
        super().__init__(option_strings, dest, **kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del namespace, values
        parser.error(f"{self._init_target} does not accept: {option_string or self.option_strings[0]}")
