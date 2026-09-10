"""Acquire bounded legacy/workspace sources for release-bound evidence expectations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_release_expectations import (
    DbtLegacyEvidencePlan,
)
from dpone.contracts.dbt_release_expectations import (
    ExpectedDbtRelease as ExpectedDbtRelease,
)
from dpone.contracts.dbt_release_expectations import (
    ExpectedDbtWorkflow as ExpectedDbtWorkflow,
)
from dpone.contracts.dbt_release_expectations import (
    expected_dbt_workflow as _expected_workflow,
)
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.manifest.confined_files import read_confined_file
from dpone.services.dbt_release_workflow_reader import (
    DbtDevEvidenceReleaseError,
)
from dpone.services.dbt_release_workflow_reader import (
    read_dbt_execution_pack as _execution_pack,
)
from dpone.services.dbt_release_workflow_reader import (
    read_dbt_workflow_dags as _workflow_dags,
)
from dpone.services.dbt_release_workflow_reader import (
    release_object as _object,
)
from dpone.services.dbt_release_workflow_reader import (
    release_text as _required_text,
)

if TYPE_CHECKING:
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader

_MAX_RELEASE_BYTES = 8 * 1024 * 1024
_MAX_SELECTION_BYTES = 1024 * 1024

DbtExpectedReleaseLoader = Callable[[Path, str], ExpectedDbtRelease]


def load_expected_dbt_release(
    compiled_root: Path,
    expected_release_id: str,
    *,
    source_reader: DbtReleaseSourceReader | None = None,
) -> ExpectedDbtRelease:
    """Read one bounded release and derive exact runtime-evidence identities."""

    root = Path(compiled_root).absolute()
    release = _object(
        read_confined_file(root, "release-set.json", max_bytes=_MAX_RELEASE_BYTES),
        "release-set",
    )
    if release.get("schema") == "dpone.release-set.v3":
        raise DbtDevEvidenceReleaseError(
            "composition requires parent-bound evidence; native-only evidence cannot certify the composed release"
        )
    producer = release.get("producer")
    if isinstance(producer, Mapping) and producer.get("wire_contract") == DBT_RUNTIME_WIRE_V2:
        if source_reader is None:
            raise DbtDevEvidenceReleaseError("workspace evidence requires a complete source reader")
        sources = source_reader.read(root, expected_release_id=expected_release_id)
        return ExpectedDbtRelease(
            release_id=sources.release_id,
            required_workloads=dict(sources.required_workloads),
            dbt_workflows={
                item.execution.workflow_id: _expected_workflow(
                    item.execution,
                    workload_pack_sha256=item.workload_pack_sha256,
                    project_bundle_sha256=item.project.project_bundle_sha256,
                    manifest_sha256=item.project.manifest_sha256,
                    workflow_dag=item.dag,
                )
                for item in sources.workflows
            },
        )
    plan = DbtLegacyEvidencePlan.from_release(release, expected_release_id)
    workflow_dags = _workflow_dags(root, plan.dag_specs, plan.workload_packs)
    project, manifest = plan.source_artifacts()
    workflows: dict[str, ExpectedDbtWorkflow] = {}
    for workload_id, descriptor in sorted(plan.workload_packs.items()):
        if not workload_id.startswith("dbt__"):
            continue
        workflow_id = workload_id.removeprefix("dbt__")
        selection = plan.selection_descriptor(workflow_id)
        selection_path = _required_text(selection.get("path"), "selection lock path")
        selection_bytes = read_confined_file(root, selection_path, max_bytes=_MAX_SELECTION_BYTES)
        lock = plan.selection_from_bytes(selection, selection_bytes, manifest)
        execution = _execution_pack(root, descriptor, workload_id)
        workflows[workflow_id] = plan.workflow_expectation(
            execution,
            workflow_id=workflow_id,
            workload_id=workload_id,
            lock=lock,
            project=project,
            manifest=manifest,
            workflow_dags=workflow_dags,
        )
    return plan.finish(workflows)


__all__ = [
    "DbtExpectedReleaseLoader",
    "DbtDevEvidenceReleaseError",
    "ExpectedDbtRelease",
    "ExpectedDbtWorkflow",
    "load_expected_dbt_release",
]
