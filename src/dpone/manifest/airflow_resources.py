"""Manifest and effective-config policy for the bounded provider resource wire."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.kubernetes_resources import KubernetesResourceError, validate_kubernetes_resources

from dpone.manifest.errors import ManifestConfigurationError

_POD_OVERRIDE_FIELDS = frozenset(
    {"pod_template_dict", "pod_template_file", "full_pod_spec", "container_resources", "resources"}
)


class AirflowResourcePlacementError(KubernetesResourceError, ManifestConfigurationError):
    """A workload resource declaration appears in a process-scoped location.

    Both resource and manifest boundaries must recognize this failure so direct
    batch loading and authoring compilation expose the same diagnostic code.
    """


def reject_process_airflow_resources(payload: Mapping[str, Any], *, field: str = "") -> None:
    """Reject resource declarations outside the manifest root before projection.

    Inspect only authoring containers, never arbitrary connector options or
    application data. Call again after folder/recipe expansion and at the batch
    compiler boundary: classic manifests can bypass the authoring compiler.
    """

    prefix = f"{field}." if field else ""
    processes = payload.get("processes")
    _reject_dynamic_container(processes, field=f"{prefix}processes")
    if isinstance(processes, list | tuple):
        for index, process in enumerate(processes):
            reject_process_resource_declaration(process, field=f"{prefix}processes[{index}]")
    reject_process_resource_declaration(payload.get("defaults"), field=f"{prefix}defaults")
    schemas = payload.get("schemas")
    _reject_dynamic_container(schemas, field=f"{prefix}schemas")
    if not isinstance(schemas, Mapping):
        return
    for name, schema in schemas.items():
        field = f"{prefix}schemas.{name}"
        reject_process_resource_declaration(schema, field=field)
        if not isinstance(schema, Mapping):
            continue
        reject_process_resource_declaration(schema.get("defaults"), field=f"{field}.defaults")
        tables = schema.get("tables")
        _reject_dynamic_container(tables, field=f"{field}.tables")
        if not isinstance(tables, list | tuple):
            continue
        for index, table in enumerate(tables):
            # String table specs are names (including templates), never process objects.
            if isinstance(table, Mapping):
                table_field = f"{field}.tables[{index}]"
                reject_process_resource_declaration(table, field=table_field)
                reject_process_resource_declaration(table.get("overrides"), field=f"{table_field}.overrides")


def reject_process_resource_declaration(value: object, *, field: str) -> None:
    """Validate resource authority before a merge can discard the declaration.

    Process-scoped GitOps/Airflow containers must be literal objects, as in the
    manifest schema. Dynamic containers could hide resources until rendering,
    or disappear altogether in merge/normalization. Other process templates and
    connector data are outside this policy. Rendered configs are checked too.
    """

    _reject_dynamic_container(value, field=field)
    if not isinstance(value, Mapping):
        return
    gitops = value.get("gitops")
    if gitops is not None and not isinstance(gitops, Mapping):
        raise _placement_error(f"{field}.gitops", "must be a literal object, not a dynamic or scalar container")
    airflow = gitops.get("airflow") if isinstance(gitops, Mapping) else None
    if airflow is not None and not isinstance(airflow, Mapping):
        raise _placement_error(f"{field}.gitops.airflow", "must be a literal object, not a dynamic or scalar container")
    if isinstance(airflow, Mapping) and "resources" in airflow:
        raise _placement_error(f"{field}.gitops.airflow.resources", "is unsupported")


def _placement_error(field: str, reason: str) -> AirflowResourcePlacementError:
    return AirflowResourcePlacementError(
        f"{field} {reason}; configure resources in gitops.airflow.resources at the manifest root. "
        "Resources apply to the whole workload."
    )


def _reject_dynamic_container(value: object, *, field: str) -> None:
    if isinstance(value, str) and ("{{" in value or "{%" in value):
        raise _placement_error(field, "must use literal authoring containers so resource placement can be validated")


def workload_airflow_resources(
    config: Mapping[str, Any], *, field: str = "airflow"
) -> dict[str, dict[str, str]] | None:
    """Read effective workload settings; absent declarations retain old defaults."""

    reject_resource_overrides(config, field=field.rsplit(".", 1)[0] if "." in field else "workload", scan_nested=False)
    if "operator_overrides" in config:
        reject_resource_overrides(config["operator_overrides"], field="operator_overrides")
    airflow = config.get("airflow")
    if airflow is None:
        return None
    if not isinstance(airflow, Mapping):
        raise KubernetesResourceError(f"{field} must be an object; configure {field}.resources.")
    reject_resource_overrides(airflow, field=field, allow_resources=True, scan_nested=False)
    for key in ("operator_overrides", "executor_config", "init_containers", "pod_override"):
        if key in airflow:
            reject_resource_overrides(airflow[key], field=f"{field}.{key}")
    if "resources" not in airflow:
        return None
    return validate_kubernetes_resources(airflow["resources"], field=f"{field}.resources")


def manifest_airflow_resources(payload: Mapping[str, Any]) -> dict[str, dict[str, str]] | None:
    """Resolve the documented manifest-local declaration before compilation."""

    reject_process_airflow_resources(payload)
    gitops = payload.get("gitops")
    if gitops is None:
        return None
    if not isinstance(gitops, Mapping):
        raise KubernetesResourceError("gitops must be an object; configure gitops.airflow.resources.")
    return workload_airflow_resources(gitops, field="gitops.airflow")


def reject_resource_overrides(
    value: object, *, field: str, allow_resources: bool = False, scan_nested: bool = True
) -> None:
    """Reject resource-bearing overrides, including nested Pod/executor inputs.

    Iterative traversal bounds work for direct Python callers as well as YAML;
    cycles and oversized objects fail closed. Only the immediate supported
    ``resources`` field can be exempted, never an arbitrary nested declaration.
    """

    pending = [(value, field, 0)]
    visited = 0
    while pending:
        current, path, depth = pending.pop()
        visited += 1
        if visited > 8000 or depth > 24:
            raise KubernetesResourceError(f"{field} exceeds supported configuration bounds; use airflow.resources.")
        if isinstance(current, Mapping):
            if len(current) > 8000:
                raise KubernetesResourceError(f"{field} exceeds supported configuration bounds; use airflow.resources.")
            for key, child in current.items():
                if depth == 0 and allow_resources and key == "resources":
                    continue
                child_path = f"{path}.{key}"
                if key in _POD_OVERRIDE_FIELDS or key in {"requests", "limits"}:
                    raise KubernetesResourceError(
                        f"{child_path} is unsupported in strict delivery; move requests/limits to workload "
                        "airflow.resources (gitops.airflow.resources in the manifest)."
                    )
                if key in {"labels", "annotations", "node_selector", "nodeSelector"}:
                    continue
                if scan_nested and isinstance(child, Mapping | list | tuple):
                    pending.append((child, child_path, depth + 1))
        elif isinstance(current, list | tuple):
            if len(current) > 8000:
                raise KubernetesResourceError(f"{field} exceeds supported configuration bounds; use airflow.resources.")
            pending.extend((child, f"{path}[{index}]", depth + 1) for index, child in enumerate(current))
