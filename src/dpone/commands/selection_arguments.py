"""Shared argparse contract for project workload selection."""

from __future__ import annotations

import argparse
import shlex


class _ExplicitSelectionLimit(argparse.Action):
    """Store the limit and remember that the user intentionally overrode it."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del parser, option_string
        setattr(namespace, self.dest, values)
        setattr(namespace, "_max_selected_explicit", True)


def add_project_selection_arguments(
    parser: argparse.ArgumentParser,
    *,
    default_max_selected: int,
) -> None:
    group = parser.add_argument_group("Advanced selection")
    group.add_argument(
        "--select",
        action="append",
        default=[],
        metavar="EXPR",
        help="Advanced: filter workloads (see Workload selectors docs)",
    )
    group.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="EXPR",
        help="Advanced: exclude workloads after selection expansion",
    )
    group.add_argument(
        "--state",
        help="Advanced: local dpone.selection-state.v1 baseline for state:* selectors",
    )
    group.add_argument(
        "--selectors",
        default="selectors.yaml",
        help="Advanced: named selector file (default: selectors.yaml)",
    )
    group.add_argument(
        "--max-selected",
        type=int,
        action=_ExplicitSelectionLimit,
        default=default_max_selected,
        help=f"Advanced: hard selected workload limit (default: {default_max_selected})",
    )


def has_project_selection(args: argparse.Namespace) -> bool:
    target = str(getattr(args, "target", "") or getattr(args, "pipeline", "") or getattr(args, "path", ""))
    return bool(
        getattr(args, "select", ())
        or getattr(args, "exclude", ())
        or getattr(args, "state", None)
        or target in {".", "./"}
    )


def selection_command(
    prefix: tuple[str, ...],
    *,
    target: str,
    args: argparse.Namespace,
) -> str:
    """Preserve the exact selected scope in one actionable next command."""

    argv = [*prefix, target]
    for expression in getattr(args, "select", ()):
        argv.extend(("--select", str(expression)))
    for expression in getattr(args, "exclude", ()):
        argv.extend(("--exclude", str(expression)))
    state = getattr(args, "state", None)
    if state:
        argv.extend(("--state", str(state)))
    selectors = str(getattr(args, "selectors", "selectors.yaml"))
    if selectors != "selectors.yaml":
        argv.extend(("--selectors", selectors))
    max_selected = getattr(args, "max_selected", None)
    if isinstance(max_selected, int) and getattr(args, "_max_selected_explicit", False):
        argv.extend(("--max-selected", str(max_selected)))
    return shlex.join(argv)


__all__ = ["add_project_selection_arguments", "has_project_selection", "selection_command"]
