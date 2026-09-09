"""Parser registration for ``dpone init dag``."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

FORMAT_CHOICES = ("text", "json", "md")
_BEGINNER_IO_CONTRACT = (
    "`0`: bounded result on stdout; stderr is empty.",
    "`1`: validation or conflict result on stdout; stderr is empty.",
    "`2`: structured application config error on stdout, or argparse syntax error on stderr.",
    "`4`: structured security or safety violation on stdout; stderr is empty.",
    "JSON failure entries conform to `dpone.error.v1`.",
)
_DAG_REJECTED_OPTIONS = (
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
    "--strategy",
    "--airflow",
    "--no-airflow",
    "--recipe",
    "--route",
    "--profile",
    "--answers",
    "--authoring",
    "--layout",
    "--owner-team",
    "--owner-contact",
    "--approver-team",
    "--from",
    "--to",
    "--key",
)


class _RejectDagOption(argparse.Action):
    def __init__(
        self,
        option_strings: Sequence[str],
        dest: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(option_strings, dest, **kwargs)

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del namespace, values
        parser.error(f"dag does not accept: {option_string or self.option_strings[0]}")


def register_dag_parser(target_parsers: argparse._SubParsersAction) -> None:
    parser = target_parsers.add_parser("dag", help="Initialize a colocated domain DAG")
    parser._dpone_io_contract = _BEGINNER_IO_CONTRACT
    parser.add_argument("name", help="DAG id (DAG__...)")
    parser.add_argument("--domain", required=True, help="Domain id for domain-first projects")
    parser.add_argument("--schedule", default=None, help="Cron schedule expression")
    parser.add_argument(
        "--pipeline",
        action="append",
        dest="pipelines",
        default=[],
        help="Pipeline id to include (repeatable)",
    )
    parser.add_argument("--description", default=None, help="Optional DAG description")
    parser.add_argument("--format", choices=FORMAT_CHOICES, default=argparse.SUPPRESS)
    for option in _DAG_REJECTED_OPTIONS:
        parser.add_argument(
            option,
            dest=f"_rejected_{option.removeprefix('--').replace('-', '_')}",
            action=_RejectDagOption,
            nargs=0 if option in {"--airflow", "--no-airflow"} else None,
            help=argparse.SUPPRESS,
        )


__all__ = ["register_dag_parser"]
