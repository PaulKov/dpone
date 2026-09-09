"""Interactive wizard helpers for ``dpone workload init``."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey


@dataclass(frozen=True, slots=True)
class WorkloadInitWizardInputs:
    source: str
    sink: str
    strategy: str
    schedule: str
    apply: bool


def resolve_workload_init_inputs(args: argparse.Namespace) -> WorkloadInitWizardInputs:
    """Fill missing route fields interactively when wizard mode is active."""

    wizard = bool(getattr(args, "wizard", False)) or _should_offer_wizard(args)
    source = _optional_str(getattr(args, "source", None))
    sink = _optional_str(getattr(args, "sink", None))
    strategy = _optional_str(getattr(args, "strategy", None)) or "full_refresh"
    schedule = _optional_str(getattr(args, "schedule", None)) or "0 6 * * *"
    apply = bool(getattr(args, "apply", False))

    if not source or not sink:
        if not wizard:
            missing = [name for name, value in (("source", source), ("sink", sink)) if not value]
            raise SystemExit(
                f"dpone workload init requires --{' and --'.join(missing)} or pass --wizard for interactive selection"
            )
        source, sink, strategy = _prompt_route(source, sink, strategy)

    catalog = RouteProfileCatalog.default()
    if catalog.get(RouteKey.of(source, sink, strategy)) is None:
        strategies = _strategies_for_pair(source, sink)
        if strategy not in strategies:
            raise SystemExit(
                f"Unsupported route {source}->{sink} with strategy {strategy!r}. Choose one of: {', '.join(strategies)}"
            )

    if wizard and sys.stdin.isatty():
        schedule = _prompt_schedule(schedule)

    if wizard and sys.stdin.isatty() and not apply:
        apply = _confirm_apply()

    return WorkloadInitWizardInputs(source=source, sink=sink, strategy=strategy, schedule=schedule, apply=apply)


def _should_offer_wizard(args: argparse.Namespace) -> bool:
    if not sys.stdin.isatty():
        return False
    return not _optional_str(getattr(args, "source", None)) or not _optional_str(getattr(args, "sink", None))


def _prompt_route(source: str | None, sink: str | None, strategy: str) -> tuple[str, str, str]:
    pairs = _certified_pairs()
    if not source:
        source = _prompt_choice("Source engine", sorted({pair[0] for pair in pairs}))
    if not sink:
        compatible = sorted({pair[1] for pair in pairs if pair[0] == source})
        if not compatible:
            raise SystemExit(f"No certified sink exists for source {source!r}")
        sink = _prompt_choice("Sink engine", compatible)
    strategies = _strategies_for_pair(source, sink)
    if strategy not in strategies:
        strategy = _prompt_choice("Strategy", strategies)
    return source, sink, strategy


def _certified_pairs() -> tuple[tuple[str, str], ...]:
    pairs = sorted({(case.source, case.sink) for case in DEFAULT_INTEGRATION_MATRIX.cases})
    return pairs


def _strategies_for_pair(source: str, sink: str) -> tuple[str, ...]:
    return tuple(sorted({case.strategy for case in DEFAULT_INTEGRATION_MATRIX.for_pair(source, sink)}))


def _prompt_choice(title: str, choices: tuple[str, ...] | list[str]) -> str:
    if not choices:
        raise SystemExit(f"No choices available for {title}")
    if len(choices) == 1:
        selected = choices[0]
        _write_line(f"{title}: {selected} (only option)")
        return selected
    _write_line(f"{title}:")
    for index, choice in enumerate(choices, start=1):
        _write_line(f"  {index}. {choice}")
    while True:
        raw = input(f"Select 1-{len(choices)}: ").strip()
        if raw.isdigit():
            position = int(raw)
            if 1 <= position <= len(choices):
                return choices[position - 1]
        if raw in choices:
            return raw
        _write_line("Enter a listed number or exact value.")


def _confirm_apply() -> bool:
    raw = input("Apply? [y/N]: ").strip().lower()
    return raw in {"y", "yes"}


def _prompt_schedule(default: str) -> str:
    raw = input(f"Schedule cron [{default}]: ").strip()
    return raw or default


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _write_line(text: str) -> None:
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


__all__ = ["WorkloadInitWizardInputs", "resolve_workload_init_inputs"]
