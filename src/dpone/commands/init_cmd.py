from __future__ import annotations

import argparse
import logging
import shlex

from dpone.commands.airflow_self_service_rendering import self_service_init_text
from dpone.commands.init_parser import (
    DEFAULT_RECIPE,
    TRACKED_OPTIONS_ATTR,
    register_parser,
)
from dpone.commands.init_route_selection import resolve_pipeline_recipe
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.manifest.errors import ManifestConfigurationError
from dpone.readiness.airflow_pipeline_init_validation import (
    parse_pipeline_id,
    safe_pipeline_id,
)
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.readiness.managed import ConnectorScaffoldService, ManagedRenderer


def cmd_init(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    if bool(getattr(args, "airflow", False)) and bool(getattr(args, "no_airflow", False)):
        init_parser = getattr(args, "_init_parser", None)
        if isinstance(init_parser, argparse.ArgumentParser):
            init_parser.error("--airflow and --no-airflow are mutually exclusive")
        raise ManifestConfigurationError("--airflow and --no-airflow are mutually exclusive")
    if args.init_target == "project":
        result = build_airflow_self_service_service().init_project(
            airflow=bool(args.airflow),
            layout=getattr(args, "layout", None),
        )
        _emit_self_service_result("dpone init project", _result_payload(result, args), args.format)
        return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)
    if args.init_target == "domain":
        result = build_airflow_self_service_service().init_domain(
            domain=args.name,
            owner_team=args.owner_team,
            owner_contact=args.owner_contact,
            approver_team=args.approver_team,
        )
        _emit_self_service_result("dpone init domain", _result_payload(result, args), args.format)
        return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)
    if args.init_target == "pipeline":
        return _init_pipeline(args)
    if args.init_target == "dag":
        return _init_dag(args)
    _reject_beginner_options_for_legacy(args)
    _require_legacy_init_args(args)
    result = ConnectorScaffoldService().generate_init_bundle(
        output_path=args.out,
        source_type=args.source_type,
        sink_type=args.sink_type,
        source_connection=args.source_connection,
        sink_connection=args.sink_connection,
        source_schema=args.source_schema,
        source_table=args.source_table,
        target_schema=args.target_schema,
        target_table=args.target_table,
        strategy=args.strategy,
        unique_key=args.unique_key,
    )
    payload = result.to_dict()
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(ManagedRenderer.render_markdown("dpone init", payload))
    else:
        write_text(ManagedRenderer.render_text("dpone init", payload))
    return 0


def _init_dag(args: argparse.Namespace) -> int:
    raw_pipelines = getattr(args, "pipelines", None) or ()
    pipelines: tuple[str, ...]
    if isinstance(raw_pipelines, str):
        pipelines = (raw_pipelines,)
    else:
        pipelines = tuple(str(item) for item in raw_pipelines)
    result = build_airflow_self_service_service().init_dag(
        dag_id=args.name,
        domain=args.domain,
        schedule=getattr(args, "schedule", None),
        pipelines=pipelines,
        description=getattr(args, "description", None),
    )
    _emit_self_service_result("dpone init dag", _result_payload(result, args), args.format)
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def _init_pipeline(args: argparse.Namespace) -> int:
    airflow_override = True if args.airflow else False if args.no_airflow else None
    route = str(getattr(args, "route", "") or "").strip() or None
    requested_recipe = str(getattr(args, "recipe", "") or "").strip() or (None if route else DEFAULT_RECIPE)
    identity = parse_pipeline_id(
        args.name,
        recipe=requested_recipe,
        route=route,
        airflow=airflow_override,
        authoring_mode=args.authoring,
        profile=args.profile,
        answers=args.answers,
    )
    if isinstance(identity, SelfServiceResult):
        _emit_self_service_result("dpone init pipeline", _result_payload(identity, args), args.format)
        return identity.exit_code or 2
    recipe = resolve_pipeline_recipe(args, default_recipe=DEFAULT_RECIPE)
    if isinstance(recipe, SelfServiceResult):
        _emit_self_service_result("dpone init pipeline", _result_payload(recipe, args), args.format)
        return recipe.exit_code or 2
    result = build_airflow_self_service_service().init_pipeline(
        pipeline_id=args.name,
        recipe=recipe,
        airflow=airflow_override,
        authoring_mode=args.authoring,
        profile=args.profile,
        answers=args.answers,
        domain=args.domain,
        from_locator=args.from_locator,
        to_locator=args.to_locator,
        unique_key=args.unique_key,
    )
    _emit_self_service_result("dpone init pipeline", _result_payload(result, args), args.format)
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def _require_legacy_init_args(args: argparse.Namespace) -> None:
    required = (
        "source_type",
        "sink_type",
        "source_connection",
        "sink_connection",
        "source_schema",
        "source_table",
        "target_schema",
        "target_table",
        "out",
    )
    missing = [f"--{name.replace('_', '-')}" for name in required if not getattr(args, name)]
    if missing:
        message = f"dpone init requires {', '.join(missing)} or one of: project, domain, pipeline, dag"
        init_parser = getattr(args, "_init_parser", None)
        if isinstance(init_parser, argparse.ArgumentParser):
            init_parser.error(message)
        raise ManifestConfigurationError(message)


def _reject_beginner_options_for_legacy(args: argparse.Namespace) -> None:
    tracked = getattr(args, TRACKED_OPTIONS_ATTR, ())
    invalid = tuple(dict.fromkeys(option for option, valid_targets in tracked if valid_targets))
    if not invalid:
        return
    message = f"legacy init does not accept: {', '.join(invalid)}"
    init_parser = getattr(args, "_init_parser", None)
    if isinstance(init_parser, argparse.ArgumentParser):
        init_parser.error(message)
    raise ManifestConfigurationError(message)


def _emit_self_service_result(title: str, payload: dict[str, object], fmt: str) -> None:
    if fmt == "json":
        write_json(payload)
    elif fmt == "md":
        write_text(_self_service_init_markdown(title, payload))
    else:
        write_text(self_service_init_text(title, payload))


def _self_service_init_markdown(title: str, payload: dict[str, object]) -> str:
    return f"# {title}\n\n{self_service_init_text(title, payload)}"


def _result_payload(result: SelfServiceResult, args: argparse.Namespace) -> dict[str, object]:
    payload = result.to_dict()
    if any(change.action == "conflict" for change in result.changes):
        payload["rerun_command"] = _rerun_command(args)
    return payload


def _rerun_command(args: argparse.Namespace) -> str:
    target = str(args.init_target)
    argv = ["dpone", "init", target]
    if target in {"domain", "pipeline", "dag"}:
        argv.append(safe_pipeline_id(args.name) if target == "pipeline" else str(args.name))
    if target == "project":
        _append_flag(argv, args, "airflow", "--airflow")
        _append_option(argv, args, "layout", "--layout")
    elif target == "domain":
        _append_option(argv, args, "owner_team", "--owner-team")
        _append_option(argv, args, "owner_contact", "--owner-contact")
        _append_option(argv, args, "approver_team", "--approver-team")
    elif target == "pipeline":
        _append_flag(argv, args, "airflow", "--airflow")
        _append_flag(argv, args, "no_airflow", "--no-airflow")
        for attr, option in (
            ("recipe", "--recipe"),
            ("route", "--route"),
            ("profile", "--profile"),
            ("answers", "--answers"),
            ("authoring", "--authoring"),
            ("domain", "--domain"),
            ("from_locator", "--from"),
            ("to_locator", "--to"),
            ("unique_key", "--key"),
        ):
            _append_option(argv, args, attr, option)
    elif target == "dag":
        _append_option(argv, args, "domain", "--domain")
        _append_option(argv, args, "schedule", "--schedule")
        _append_option(argv, args, "description", "--description")
        raw_pipelines = getattr(args, "pipelines", None) or ()
        values = (raw_pipelines,) if isinstance(raw_pipelines, str) else tuple(raw_pipelines)
        for pipeline_id in values:
            if pipeline_id:
                argv.extend(("--pipeline", str(pipeline_id)))
    return shlex.join(argv)


def _append_flag(argv: list[str], args: argparse.Namespace, attr: str, option: str) -> None:
    if bool(getattr(args, attr, False)):
        argv.append(option)


def _append_option(argv: list[str], args: argparse.Namespace, attr: str, option: str) -> None:
    value = getattr(args, attr, None)
    if value is not None and str(value):
        argv.extend((option, str(value)))


__all__ = ["cmd_init", "register_parser"]
