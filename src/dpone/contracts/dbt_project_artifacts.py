"""Captured dbt source, runtime projection and checked-report identity.

These immutable process-local snapshots perform no I/O and grant no publication,
signature or live certification authority. Services acquire and verify bytes.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from functools import partial
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_execution_pack import DbtExecutionPack, DbtProfileSpec
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V1,
    DBT_RUNTIME_WIRE_V2,
    dbt_runtime_payload_reference,
    dbt_runtime_payload_trio,
)
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)
from dpone.contracts.dbt_sqlserver_policy import DbtSqlServerAdapterPolicy, DbtSqlServerRuntimePolicy
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.dbt_workflow_graph_policy import evaluate_dbt_workflow_graph_ownership

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import CompiledDbtWorkflow, DbtCompileReport, DbtPublishProfile
    from dpone.contracts.dbt_selection import ResolvedDbtSelection


@dataclass(frozen=True, slots=True)
class DbtWorkflowReleasePlan:
    """One workflow's domain inputs around the external selection boundary.

    Runtime options are captured before selection; quality is intentionally read
    after the callback, preserving the legacy construction order. The enclosing
    projector still rejects any changed checked report before publication.
    """

    workflow: CompiledDbtWorkflow
    profile: DbtPublishProfile
    runtime: Mapping[str, Any]
    profile_name: str
    target_name: str
    selected_models: tuple[str, ...]
    database: str
    schema: str
    invocation_context: DbtInvocationContext
    timeout_seconds: int
    adapter_runtime: DbtSqlServerRuntimePolicy

    @classmethod
    def prepare(cls, workflow: CompiledDbtWorkflow) -> DbtWorkflowReleasePlan:
        profile = workflow.models[0].profile
        runtime = dict(profile.runtime)
        profile_name = str(runtime.get("dbt_profile") or "dpone_runtime")
        target_name = str(runtime.get("dbt_target") or "runtime")
        selected_models = tuple(item.model.unique_id for item in workflow.models)
        database, schema = workflow_logical_target(workflow)
        invocation_context = DbtInvocationContext.canonical()
        timeout_seconds = positive_int(runtime.get("dbt_timeout_seconds"), default=3600)
        adapter_runtime = DbtSqlServerRuntimePolicy.for_process_timeout(timeout_seconds)
        return cls(
            workflow,
            profile,
            MappingProxyType(runtime),
            profile_name,
            target_name,
            selected_models,
            database,
            schema,
            invocation_context,
            timeout_seconds,
            adapter_runtime,
        )

    def selection_arguments(self) -> dict[str, Any]:
        """Domain arguments only; the service supplies acquired snapshot paths."""

        return {
            "selected_unique_ids": self.selected_models,
            "profile_name": self.profile_name,
            "target_name": self.target_name,
            "dbt_core_version": DBT_SQLSERVER_1_12_CERTIFIED.dbt_core_version,
            "dbt_adapter": DBT_SQLSERVER_1_12_CERTIFIED.adapter_name,
            "dbt_adapter_version": DBT_SQLSERVER_1_12_CERTIFIED.adapter_version,
        }

    def complete(
        self,
        selection: ResolvedDbtSelection,
        *,
        manifest_sha256: str,
        toolchain_sha256: str,
        project_sha256: str,
        wire_contract: str,
    ) -> tuple[DbtSelectionLock, DbtExecutionPack]:
        """Build lock first, then versioned pack, retaining failure precedence."""

        if wire_contract not in (DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2):
            raise ValueError("unsupported dbt release wire contract")
        lock = DbtSelectionLock.build(
            manifest_sha256=manifest_sha256,
            toolchain_sha256=toolchain_sha256,
            invocation_context_sha256=self.invocation_context.invocation_context_sha256,
            graph_contract_sha256=selection.graph_contract_sha256,
            graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
            graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
            selectors=selection.selectors,
            selected_graph_unique_ids=selection.selected_graph_unique_ids,
            expected_run_result_unique_ids=selection.expected_run_result_unique_ids,
            publish_model_unique_ids=self.selected_models,
        )
        build_pack = DbtExecutionPack.build
        if wire_contract == DBT_RUNTIME_WIRE_V2:
            if selection.invocation_target is None:
                raise ValueError("workspace release requires the dbt-rendered invocation target")
            build_pack = partial(DbtExecutionPack.build_v2, invocation_target=selection.invocation_target)
        pack = build_pack(
            workflow_id=self.workflow.workflow,
            project_bundle_sha256=project_sha256,
            project_subdir="dbt-project",
            target_path="target",
            profile=DbtProfileSpec(
                profile_name=self.profile_name,
                target_name=self.target_name,
                connection_ref=self.profile.source_connection_ref,
                adapter_type=DBT_SQLSERVER_1_12_CERTIFIED.adapter_name,
                database=self.database,
                schema=self.schema,
                threads=positive_int(self.runtime.get("dbt_threads"), default=4),
            ),
            selection_lock=lock,
            invocation_context=self.invocation_context,
            adapter_runtime=self.adapter_runtime,
            adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
            dbt_warning_policy=dbt_warning_policy(self.profile.quality),
            timeout_seconds=self.timeout_seconds,
        )
        return lock, pack


@dataclass(frozen=True, slots=True)
class DbtReleaseInputs:
    """Immutable build inputs shared by workload and release projections."""

    project_root: Path
    manifest_bytes: bytes
    project_archive: bytes
    project_sha256: str
    toolchain_sha256: str
    selection_locks: Mapping[str, DbtSelectionLock]
    execution_packs: Mapping[str, DbtExecutionPack]
    selection_authority: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "selection_locks", MappingProxyType(dict(self.selection_locks)))
        object.__setattr__(self, "execution_packs", MappingProxyType(dict(self.execution_packs)))


@dataclass(frozen=True, slots=True)
class DbtProjectRuntimePayloads:
    files: Mapping[str, bytes]
    descriptors: tuple[Mapping[str, object], ...]
    workflow_ids: Mapping[str, tuple[str, ...]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "files", MappingProxyType(dict(self.files)))
        object.__setattr__(self, "descriptors", tuple(MappingProxyType(dict(row)) for row in self.descriptors))
        object.__setattr__(self, "workflow_ids", MappingProxyType(dict(self.workflow_ids)))


def project_runtime_payloads(inputs: DbtReleaseInputs, *, wire_contract: str) -> DbtProjectRuntimePayloads:
    files: dict[str, bytes] = {}
    descriptors: dict[str, Mapping[str, object]] = {}
    workflows = {}
    # Legacy callers can project the two shared sources before any workflow
    # lock exists. Keep their descriptor order and paths byte-compatible.
    if wire_contract == DBT_RUNTIME_WIRE_V1:
        for payload_id, content in (("dbt_project", inputs.project_archive), ("dbt_manifest", inputs.manifest_bytes)):
            reference = dbt_runtime_payload_reference(payload_id, wire_contract=wire_contract)
            files[reference.path] = content
            descriptors[payload_id] = reference.descriptor(content)
    for workflow, lock in sorted(inputs.selection_locks.items()):
        # The selection object's byte hash differs from its semantic fingerprint.
        selection = (json.dumps(lock.to_dict(), allow_nan=False, ensure_ascii=False, sort_keys=True) + "\n").encode()
        ids = dbt_runtime_payload_trio(
            workflow_id=workflow,
            project_sha256=inputs.project_sha256,
            manifest_sha256=lock.manifest_sha256,
            selection_lock_payload=selection,
            wire_contract=wire_contract,
        )
        workflows[workflow] = ids
        for payload_id, content in zip(ids, (inputs.project_archive, inputs.manifest_bytes, selection), strict=True):
            reference = dbt_runtime_payload_reference(payload_id, wire_contract=wire_contract)
            descriptor = reference.descriptor(content)
            if payload_id in descriptors and descriptors[payload_id] != descriptor:
                raise ValueError("runtime identity maps to conflicting descriptors")
            if reference.path in files and files[reference.path] != content:
                raise ValueError("runtime path maps to conflicting bytes")
            files[reference.path] = content
            descriptors[payload_id] = descriptor
    return DbtProjectRuntimePayloads(files, tuple(descriptors.values()), workflows)


@dataclass(frozen=True, slots=True)
class DbtProjectArtifacts:
    """One source snapshot and immutable inventories, without a nested release."""

    inputs: DbtReleaseInputs
    files: Mapping[str, bytes]
    artifacts: Mapping[str, str]
    pack_files: Mapping[str, bytes]
    dag_files: Mapping[str, bytes]
    runtime_payload_ids: Mapping[str, tuple[str, ...]]
    payloads: DbtProjectRuntimePayloads
    checked_report_sha256: str
    route_certifications_payload: bytes

    def __post_init__(self) -> None:
        for name in ("files", "artifacts", "pack_files", "dag_files", "runtime_payload_ids"):
            object.__setattr__(self, name, MappingProxyType(dict(getattr(self, name))))

    @property
    def runtime_files(self) -> Mapping[str, bytes]:
        return self.payloads.files

    def route_certifications(self, report: DbtCompileReport) -> list[dict[str, Any]]:
        """Return detached receipts only for the exact originating report.

        The receipt bytes and full input identity are process-local projection
        metadata, not a new release wire or a certification authority.
        """

        if _report_identity(report) != self.checked_report_sha256:
            raise ValueError("project certification/input conflict: checked report changed after projection")
        return json.loads(self.route_certifications_payload)


def _report_identity(report: DbtCompileReport) -> str:
    """Bind full typed inputs, including fields omitted by the user-facing report.

    Dataclass fields and immutable mapping views are normalized without deepcopy
    (semantic profiles contain mapping proxies). Unknown values fail closed.
    """

    def encode(value: object) -> object:
        if is_dataclass(value) and not isinstance(value, type):
            return {field.name: getattr(value, field.name) for field in fields(value)}
        if isinstance(value, Mapping):
            return dict(value)
        raise TypeError("unsupported checked dbt report value")

    digest = hashlib.sha256()
    encoder = json.JSONEncoder(default=encode, allow_nan=False, sort_keys=True, separators=(",", ":"))
    for chunk in encoder.iterencode(report):
        digest.update(chunk.encode())
    return "sha256:" + digest.hexdigest()


def release_toolchain_sha256(report: DbtCompileReport) -> str:
    observed = (
        report.dbt_version,
        report.dbt_adapter,
        report.dbt_adapter_version,
    )
    expected = (
        DBT_SQLSERVER_1_12_CERTIFIED.dbt_core_version,
        DBT_SQLSERVER_1_12_CERTIFIED.adapter_name,
        DBT_SQLSERVER_1_12_CERTIFIED.adapter_version,
    )
    if observed != expected:
        raise ValueError("exact dbt Core and adapter versions are required for release materialization")
    return DBT_SQLSERVER_1_12_CERTIFIED.sha256


def workflow_logical_target(
    workflow: CompiledDbtWorkflow,
) -> tuple[str, str]:
    targets = {
        (
            str(item.model.relation.get("database") or ""),
            str(item.model.relation.get("schema") or ""),
        )
        for item in workflow.models
    }
    if len(targets) != 1:
        raise ValueError("one dbt workflow must use one logical database and schema")
    database, schema = targets.pop()
    if not database or not schema:
        raise ValueError("dbt workflow logical target is incomplete")
    return database, schema


def require_dbt_workflow_graph_ownership(
    *,
    publish_model_ids_by_workflow: Mapping[str, tuple[str, ...]],
    selected_graph_ids_by_workflow: Mapping[str, tuple[str, ...]],
    model_paths: Mapping[str, str],
) -> None:
    report = evaluate_dbt_workflow_graph_ownership(
        publish_model_ids_by_workflow=publish_model_ids_by_workflow,
        selected_graph_ids_by_workflow=selected_graph_ids_by_workflow,
    )
    if report.passed:
        return
    issue = report.issues[0]
    raise DbtPublishingError(
        issue.code,
        issue.message,
        path=model_paths.get(issue.unique_id, issue.path),
        remediation=issue.remediation,
    )


def positive_int(value: object, *, default: int) -> int:
    """Return one positive integer runtime option or its default."""

    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("dbt runtime integer setting must be positive")
    return value


def dbt_warning_policy(quality: Mapping[str, Any]) -> str:
    """Return the closed dbt warning policy."""

    value = quality.get("dbt_warning_policy", "fail")
    if value not in {"fail", "allow"}:
        raise ValueError("dbt warning policy must be fail or allow")
    return str(value)
