from __future__ import annotations

import argparse
import tempfile
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.gitops.pack_storage_mode import (
    pack_storage_policy_from_workload_set,
    validate_pack_storage_policy,
)
from dpone.output_json import dumps_json, write_json


@dataclass(frozen=True, slots=True)
class _PublishInputs:
    uri_prefix: str
    latest_index_uri: str
    connection_id: str


def run_publish(args: argparse.Namespace, *, ctx: object) -> int:
    repo_root = Path(getattr(getattr(ctx, "settings"), "repo_root"))
    try:
        storage_policy = pack_storage_policy_from_workload_set(
            _load_workload_set(repo_root=repo_root, workload_set=str(args.workload_set))
        )
        validate_pack_storage_policy(storage_policy)
    except ValueError as exc:
        return _blocked_publish(
            args, ctx=ctx, code="airflow_publish_storage_policy_invalid", extra={"message": str(exc)}
        )
    if storage_policy.mode == "gitops":
        return _blocked_publish(args, ctx=ctx, code="airflow_publish_storage_mode_gitops")
    publish_inputs = _publish_inputs(args, repo_root=repo_root)
    if not publish_inputs.uri_prefix or not publish_inputs.latest_index_uri:
        return _blocked_publish(args, ctx=ctx, code="airflow_publish_artifact_storage_missing")
    if not args.local_root_dir and not publish_inputs.connection_id:
        return _blocked_publish(args, ctx=ctx, code="airflow_publish_writer_connection_missing")
    workload_type = getattr(
        import_module("dpone.gitops.workload_catalog_models"),
        "GitOpsWorkloadDefinition",
    )
    workloads = _selected_publish_workloads(args, repo_root=repo_root, workload_type=workload_type)
    if not workloads:
        return _blocked_publish(args, ctx=ctx, code="airflow_publish_no_workloads")
    builder = getattr(import_module("dpone.gitops.airflow_compact_pack"), "AirflowCompactPackBuilder")()
    with tempfile.TemporaryDirectory(prefix="dpone-airflow-pack-publish-") as tmp_raw:
        packs, missing = _resolve_publish_packs(
            args,
            workloads=workloads,
            repo_root=repo_root,
            tmp_dir=Path(tmp_raw),
            builder=builder,
        )
        if missing:
            return _blocked_publish(
                args,
                ctx=ctx,
                code="airflow_publish_missing_packs",
                extra={"missing_workloads": missing},
            )
        payload = _publish_packs(
            args,
            packs=packs,
            dag_specs=_collect_publish_dag_specs(repo_root),
            publish_inputs=publish_inputs,
        )
    _emit_publish(args, ctx=ctx, payload=payload)
    return 0 if payload.get("passed") else 1


def _publish_inputs(args: argparse.Namespace, *, repo_root: Path) -> _PublishInputs:
    artifact_storage = _artifact_storage_config(repo_root=repo_root, workload_set=str(args.workload_set))
    return _PublishInputs(
        uri_prefix=str(args.uri_prefix or artifact_storage.get("uri_prefix") or ""),
        latest_index_uri=str(args.latest_index_uri or artifact_storage.get("latest_index_uri") or ""),
        connection_id=str(args.connection_id or artifact_storage.get("writer_connection_id") or ""),
    )


def _selected_publish_workloads(args: argparse.Namespace, *, repo_root: Path, workload_type: type) -> list:
    catalog = (
        import_module("dpone.gitops.workload_catalog")
        .WorkloadCatalogResolver(
            repo_root=repo_root,
        )
        .resolve(args.workload_set, env=str(args.env))
    )
    requested = {str(item) for item in args.workload}
    return [item for item in catalog.workloads if not requested or item.workload_id in requested]


def _resolve_publish_packs(
    args: argparse.Namespace,
    *,
    workloads: list,
    repo_root: Path,
    tmp_dir: Path,
    builder: Any,
) -> tuple[dict[str, Path], list[str]]:
    pack_dir = getattr(args, "pack_dir", None)
    if pack_dir:
        root = repo_root / str(pack_dir)
        packs: dict[str, Path] = {}
        missing: list[str] = []
        for workload in workloads:
            path = root / workload.workload_id / "airflow-pack.json"
            if path.exists():
                packs[workload.workload_id] = path
            else:
                missing.append(workload.workload_id)
        return packs, missing
    return _build_publish_packs(workloads, repo_root=repo_root, tmp_dir=tmp_dir, builder=builder), []


def _build_publish_packs(
    workloads: list,
    *,
    repo_root: Path,
    tmp_dir: Path,
    builder: Any,
) -> dict[str, Path]:
    packs: dict[str, Path] = {}
    for workload in workloads:
        workload_id = workload.workload_id
        report = builder.build(
            workload=workload,
            output_path=f"airflow/{workload_id}/airflow-pack.json",
            repo_root=repo_root,
            runner_policy=str(workload.effective_config.get("runner_policy") or "advisory"),
        )
        path = tmp_dir / workload_id / "airflow-pack.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report.to_json(), encoding="utf-8")
        packs[workload_id] = path
    return packs


def _publish_packs(
    args: argparse.Namespace,
    *,
    packs: dict[str, Path],
    dag_specs: dict[str, Path],
    publish_inputs: _PublishInputs,
) -> dict[str, object]:
    publisher_module = import_module("dpone.gitops.airflow_pack_publisher")
    store_options = {
        **vars(args),
        "connection_id": publish_inputs.connection_id,
        "uri_prefix": publish_inputs.uri_prefix,
    }
    store = publisher_module.build_object_artifact_store(store_options, uri=publish_inputs.uri_prefix)
    publisher = publisher_module.AirflowPackPublisher(
        store=store,
        index_builder=publisher_module.AirflowPackIndexBuilder(
            max_pack_bytes=int(args.max_pack_bytes),
            max_index_bytes=int(args.max_index_bytes),
        ),
    )
    return publisher.publish(
        packs=packs,
        uri_prefix=publish_inputs.uri_prefix,
        latest_index_uri=publish_inputs.latest_index_uri,
        git_sha=str(args.git_sha),
        env=str(args.env),
        dag_specs=dag_specs,
    )


def _load_workload_set(*, repo_root: Path, workload_set: str) -> dict[str, Any]:
    yaml = import_module("yaml")
    path = repo_root / workload_set
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _artifact_storage_config(*, repo_root: Path, workload_set: str) -> dict[str, object]:
    payload = _load_workload_set(repo_root=repo_root, workload_set=workload_set)
    gitops = payload.get("gitops")
    if not isinstance(gitops, dict):
        return {}
    artifacts = gitops.get("artifacts")
    if not isinstance(artifacts, dict):
        return {}
    airflow_pack = artifacts.get("airflow_pack")
    if not isinstance(airflow_pack, dict):
        return {}
    storage = airflow_pack.get("storage")
    return dict(storage) if isinstance(storage, dict) else {}


def _collect_publish_dag_specs(repo_root: Path) -> dict[str, Path]:
    spec_dir = repo_root / ".dpone" / "gitops" / "airflow" / "_dags"
    if not spec_dir.is_dir():
        return {}
    specs: dict[str, Path] = {}
    for path in sorted(spec_dir.glob("*.dag-spec.json")):
        specs[path.name[: -len(".dag-spec.json")]] = path
    return specs


def _blocked_publish(
    args: argparse.Namespace,
    *,
    ctx: object,
    code: str,
    extra: dict[str, object] | None = None,
) -> int:
    payload: dict[str, object] = {
        "kind": "gitops.airflow_pack_publish",
        "schema_version": "1",
        "git_sha": str(args.git_sha),
        "blockers": [code],
        "passed": False,
    }
    if extra:
        payload.update(extra)
    _emit_publish(args, ctx=ctx, payload=payload)
    return 1


def _emit_publish(args: argparse.Namespace, *, ctx: object, payload: dict[str, object]) -> None:
    rendered = dumps_json(payload)
    common = import_module("dpone.commands.gitops.common")
    common.write_optional_output(ctx, getattr(args, "output", None), rendered)
    write_json(payload)


__all__ = ["run_publish"]
