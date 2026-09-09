"""Target-scoped parser composition for ``dpone init``."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

BEGINNER_TARGETS = frozenset({"project", "domain", "pipeline", "dag"})
TRACKED_OPTIONS_ATTR = "_init_options_before_target"
DEFAULT_RECIPE = "mssql-to-clickhouse-incremental"
AUTHORING_CHOICES = ("classic", "flow", "folder")
FORMAT_CHOICES = ("text", "json", "md")
_BEGINNER_IO_CONTRACT = (
    "`0`: bounded result on stdout; stderr is empty.",
    "`1`: validation or conflict result on stdout; stderr is empty.",
    "`2`: structured application config error on stdout, or argparse syntax error on stderr.",
    "`4`: structured security or safety violation on stdout; stderr is empty.",
    "JSON failure entries conform to `dpone.error.v1`.",
)
_PROJECT_EPILOG = """\
Domain-first first steps:
  dpone init project --airflow --layout domain-first
  dpone init domain crm --owner-team data-crm --owner-contact crm@example.com --approver-team data-platform
  dpone init pipeline orders_daily --domain crm --route mssql:clickhouse:incremental_merge \\
    --from mssql_dev:dbo.orders --to clickhouse_dev:analytics.orders --key order_id
  dpone init dag DAG__crm__orders__refresh --domain crm --schedule "0 6 * * *" \\
    --pipeline orders_daily

Use `dpone init <target> --help` for target-specific options.
"""

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
_PROJECT_REJECTED_OPTIONS = (
    *_LEGACY_VALUE_OPTIONS,
    "--strategy",
    "--recipe",
    "--route",
    "--profile",
    "--answers",
    "--authoring",
    "--no-airflow",
    "--owner-team",
    "--owner-contact",
    "--approver-team",
    "--domain",
    "--from",
    "--to",
    "--key",
    "--schedule",
    "--pipeline",
    "--description",
)
_DOMAIN_REJECTED_OPTIONS = (
    *_LEGACY_VALUE_OPTIONS,
    "--strategy",
    "--airflow",
    "--no-airflow",
    "--recipe",
    "--route",
    "--profile",
    "--answers",
    "--authoring",
    "--layout",
    "--domain",
    "--from",
    "--to",
    "--key",
    "--schedule",
    "--pipeline",
    "--description",
)
_PIPELINE_REJECTED_OPTIONS = (
    *_LEGACY_VALUE_OPTIONS,
    "--strategy",
    "--layout",
    "--owner-team",
    "--owner-contact",
    "--approver-team",
    "--schedule",
    "--pipeline",
    "--description",
)


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


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    from dpone.commands.init_dag_parser import register_dag_parser

    parser = subparsers.add_parser(
        "init",
        help="Initialize a project, domain, pipeline, dag, or legacy manifest bundle",
        description="Initialize beginner self-service authority or a legacy manifest bundle.",
    )
    airflow_group = parser.add_mutually_exclusive_group()
    airflow_group.add_argument(
        "--airflow",
        action=_TrackedInitOption,
        nargs=0,
        const=True,
        default=False,
        help=argparse.SUPPRESS,
    )
    airflow_group.add_argument(
        "--no-airflow",
        dest="no_airflow",
        action=_TrackedInitOption,
        nargs=0,
        const=True,
        default=False,
        help=argparse.SUPPRESS,
    )
    for option in ("--recipe", "--route", "--profile", "--answers"):
        parser.add_argument(option, action=_TrackedInitOption, default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--authoring",
        action=_TrackedInitOption,
        choices=AUTHORING_CHOICES,
        default="flow",
        help=argparse.SUPPRESS,
    )
    for option, dest in (
        ("--layout", "layout"),
        ("--owner-team", "owner_team"),
        ("--owner-contact", "owner_contact"),
        ("--approver-team", "approver_team"),
        ("--domain", "domain"),
        ("--from", "from_locator"),
        ("--to", "to_locator"),
        ("--key", "unique_key"),
        ("--schedule", "schedule"),
        ("--description", "description"),
    ):
        parser.add_argument(option, action=_TrackedInitOption, dest=dest, help=argparse.SUPPRESS)
    parser.add_argument(
        "--pipeline",
        action=_TrackedInitOption,
        dest="pipelines",
        default=[],
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--format", choices=FORMAT_CHOICES, default="text")

    parser.register("action", "init_target_parsers", _InitTargetParsersAction)
    target_parsers = parser.add_subparsers(
        dest="init_target",
        action="init_target_parsers",
        title="beginner targets",
        description="Omit a target to use legacy manifest bundle options.",
        metavar="[{project,domain,pipeline,dag}]",
    )
    _register_project_parser(target_parsers)
    _register_domain_parser(target_parsers)
    _register_pipeline_parser(target_parsers)
    register_dag_parser(target_parsers)
    _register_legacy_options(parser)
    parser.set_defaults(_init_parser=parser)
    return parser


def _register_project_parser(target_parsers: argparse._SubParsersAction) -> None:
    parser = target_parsers.add_parser(
        "project",
        help="Initialize a beginner Airflow project",
        epilog=_PROJECT_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser._dpone_io_contract = _BEGINNER_IO_CONTRACT
    parser.add_argument("--airflow", action="store_true", default=argparse.SUPPRESS, help="Include Airflow files")
    parser.add_argument("--format", choices=FORMAT_CHOICES, default=argparse.SUPPRESS)
    parser.add_argument(
        "--layout",
        choices=("flat", "domain-first"),
        default=argparse.SUPPRESS,
        help="Project authoring layout (default: flat compatibility)",
    )
    _add_rejected_options(parser, target="project", options=_PROJECT_REJECTED_OPTIONS)


def _register_domain_parser(target_parsers: argparse._SubParsersAction) -> None:
    parser = target_parsers.add_parser("domain", help="Initialize domain ownership")
    parser._dpone_io_contract = _BEGINNER_IO_CONTRACT
    parser.add_argument("name", help="Canonical domain id")
    parser.add_argument("--owner-team", required=True, help="Owning team")
    parser.add_argument("--owner-contact", required=True, help="Owner contact")
    parser.add_argument("--approver-team", required=True, help="Approving GitHub team")
    parser.add_argument("--format", choices=FORMAT_CHOICES, default=argparse.SUPPRESS)
    _add_rejected_options(parser, target="domain", options=_DOMAIN_REJECTED_OPTIONS)


def _register_pipeline_parser(target_parsers: argparse._SubParsersAction) -> None:
    parser = target_parsers.add_parser("pipeline", help="Initialize a beginner Airflow pipeline")
    parser._dpone_io_contract = _BEGINNER_IO_CONTRACT
    parser.add_argument("name", help="Pipeline name")
    airflow_group = parser.add_mutually_exclusive_group()
    airflow_group.add_argument(
        "--airflow",
        action=_TrackedInitOption,
        nargs=0,
        const=True,
        default=argparse.SUPPRESS,
        help="Include Airflow self-service files",
    )
    airflow_group.add_argument(
        "--no-airflow",
        dest="no_airflow",
        action=_TrackedInitOption,
        nargs=0,
        const=True,
        default=argparse.SUPPRESS,
        help="Do not include this pipeline in Airflow",
    )
    recipe_group = parser.add_mutually_exclusive_group()
    recipe_group.add_argument(
        "--recipe",
        default=argparse.SUPPRESS,
        help="Built-in id or exact external recipe ref (id@MAJOR.MINOR.PATCH)",
    )
    recipe_group.add_argument(
        "--route",
        default=argparse.SUPPRESS,
        help="Scaffoldable source:sink:strategy route",
    )
    parser.add_argument(
        "--profile",
        default=argparse.SUPPRESS,
        help="Exact external recipe profile ref (id@MAJOR.MINOR.PATCH)",
    )
    parser.add_argument(
        "--answers",
        default=argparse.SUPPRESS,
        help="Project-confined declarative YAML recipe answers",
    )
    parser.add_argument(
        "--authoring",
        choices=AUTHORING_CHOICES,
        default=argparse.SUPPRESS,
        help="Primary authoring mode (default: flow)",
    )
    parser.add_argument("--domain", default=argparse.SUPPRESS, help="Domain id for domain-first projects")
    parser.add_argument(
        "--from",
        dest="from_locator",
        default=argparse.SUPPRESS,
        help="Source locator: connection_ref:schema.table",
    )
    parser.add_argument(
        "--to",
        dest="to_locator",
        default=argparse.SUPPRESS,
        help="Sink locator: connection_ref:schema.table",
    )
    parser.add_argument(
        "--key",
        dest="unique_key",
        default=argparse.SUPPRESS,
        help="Unique key for merge recipes",
    )
    parser.add_argument("--format", choices=FORMAT_CHOICES, default=argparse.SUPPRESS)
    _add_rejected_options(parser, target="pipeline", options=_PIPELINE_REJECTED_OPTIONS)


def _register_legacy_options(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group(
        "legacy manifest bundle (advanced)",
        "Compatibility options for the original `dpone init` surface.",
    )
    for option in _LEGACY_VALUE_OPTIONS:
        group.add_argument(option, action=_TrackedInitOption)
    group.add_argument("--strategy", action=_TrackedInitOption, default="full_refresh")


def _add_rejected_options(
    parser: argparse.ArgumentParser,
    *,
    target: str,
    options: Sequence[str],
) -> None:
    for option in options:
        parser.add_argument(
            option,
            dest=f"_rejected_{option.removeprefix('--').replace('-', '_')}",
            action=_RejectInitOption,
            init_target=target,
            nargs=0 if option in {"--airflow", "--no-airflow"} else None,
            help=argparse.SUPPRESS,
        )


__all__ = ["BEGINNER_TARGETS", "DEFAULT_RECIPE", "TRACKED_OPTIONS_ATTR", "register_parser"]
