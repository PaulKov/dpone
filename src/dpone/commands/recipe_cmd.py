"""Thin CLI facade for declarative recipe discovery and validation."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.contracts.connector_declarations import canonical_connector_id


def recipe_group() -> CommandGroup:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("recipe", help="Discover and validate declarative recipes")

    return CommandGroup(
        name="recipe",
        help="Discover and validate declarative recipes",
        build_parser=build,
        subcommands=(
            FuncCommand("list", register_list_parser, cmd_list),
            FuncCommand("show", register_show_parser, cmd_show),
            FuncCommand("pin", register_pin_parser, cmd_pin),
            FuncCommand("validate", register_validate_parser, cmd_validate),
        ),
        subdest="recipe_command",
    )


def cmd_list(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _execute(args, lambda service: _recipe_list_payload(args, service))


def cmd_show(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _execute(args, lambda service: _recipe_show_payload(args.ref, service))


def cmd_pin(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _execute(args, lambda service: service.pin_artifact(Path(args.artifact)))


def cmd_validate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _execute(args, lambda service: service.validate_catalog())


def register_list_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("list", help="List built-in and configured recipe refs")
    parser.add_argument("--source", help="Filter by source endpoint family")
    parser.add_argument("--sink", help="Filter by sink endpoint family")
    parser.add_argument("--strategy", help="Filter by load strategy")
    _format_arg(parser)
    return parser


def register_show_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("show", help="Show safe metadata and parameters for one recipe")
    parser.add_argument("ref")
    _format_arg(parser)
    return parser


def register_pin_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("pin", help="Validate an artifact and print its exact catalog pin")
    parser.add_argument("artifact")
    _format_arg(parser)
    return parser


def register_validate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("validate", help="Validate the configured catalog and pinned closures")
    _format_arg(parser)
    return parser


def _format_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", choices=["text", "json"], default="text")


def _execute(args: argparse.Namespace, operation: Callable[[Any], dict[str, Any]]) -> int:
    models = import_module("dpone.manifest.recipe_models")
    errors = import_module("dpone.readiness.airflow_recipe_errors")
    operations = import_module("dpone.manifest.recipe_catalog_operations")
    templates = import_module("dpone.readiness.airflow_self_service_templates")
    capabilities = import_module("dpone.readiness.capability_discovery_service")
    try:
        service = operations.RecipeCatalogOperations(Path.cwd(), built_in_recipes=templates.RECIPE_DEFAULTS)
        payload = {"passed": True, **operation(service)}
    except (models.RecipeResolutionError, capabilities.CapabilityDiscoveryError) as exc:
        recipe_ref = str(getattr(args, "ref", "catalog"))
        command = str(getattr(args, "recipe_command", "operation"))
        payload = {
            "passed": False,
            "errors": [
                errors.recipe_error(
                    exc.code,
                    str(exc),
                    stage=f"recipe_{command}",
                    recipe_ref=recipe_ref,
                )
            ],
        }
        _render(payload, fmt=args.format)
        return int(errors.recipe_error_exit_code(exc.code))
    failed = payload.get("passed") is False
    _render(payload, fmt=args.format)
    return 1 if failed else 0


def _recipe_list_payload(args: argparse.Namespace, service: Any) -> dict[str, Any]:
    snapshot = _capability_snapshot(service)
    source = _validated_filter(
        getattr(args, "source", None),
        allowed={item.source for item in snapshot.routes},
        kind="source",
    )
    sink = _validated_filter(
        getattr(args, "sink", None),
        allowed={item.sink for item in snapshot.routes},
        kind="sink",
    )
    strategy = _validated_filter(
        getattr(args, "strategy", None),
        allowed={item.strategy for item in snapshot.routes},
        kind="strategy",
    )
    recipes = [
        item.to_dict()
        for item in snapshot.recipes
        if _matches_filter(item.source, source)
        and _matches_filter(item.sink, sink)
        and _matches_filter(item.strategy, strategy)
    ]
    routes = [
        item.to_dict()
        for item in snapshot.routes
        if _matches_filter(item.source, source)
        and _matches_filter(item.sink, sink)
        and _matches_filter(item.strategy, strategy)
    ]
    issues = [item.to_dict() for item in snapshot.issues]
    return {
        "passed": not issues,
        "snapshot_id": snapshot.snapshot_id,
        "routes": routes,
        "recipes": recipes,
        "issues": issues,
    }


def _recipe_show_payload(ref: str, service: Any) -> dict[str, Any]:
    payload = service.show_recipe(ref)
    snapshot = _capability_snapshot(service)
    capability = next((item for item in snapshot.recipes if item.ref == ref), None)
    issues = [item.to_dict() for item in snapshot.issues]
    return {
        "passed": not issues,
        "snapshot_id": snapshot.snapshot_id,
        **payload,
        **(capability.to_dict() if capability is not None else {}),
        "issues": issues,
    }


def _capability_snapshot(service: Any) -> Any:
    capabilities = import_module("dpone.readiness.capability_discovery_composition")
    return capabilities.build_capability_discovery_service(
        root=Path.cwd(),
        recipe_operations=service,
    ).snapshot()


def _matches_filter(actual: str | None, expected: str | None) -> bool:
    if expected is None:
        return True
    return actual == expected


def _validated_filter(
    value: str | None,
    *,
    allowed: set[str],
    kind: str,
) -> str | None:
    if value is None:
        return None
    normalized = (
        canonical_connector_id(value) if kind in {"source", "sink"} else value.strip().lower().replace("-", "_")
    )
    if normalized not in allowed:
        errors = import_module("dpone.readiness.capability_discovery_service")
        raise errors.CapabilityDiscoveryError(
            "DPONE_ROUTE_FILTER_INVALID",
            f"Unknown recipe {kind} filter.",
        )
    return normalized


def _render(payload: dict[str, Any], *, fmt: str) -> None:
    if fmt == "json":
        write_json(payload)
    else:
        write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))


__all__ = ["recipe_group"]
