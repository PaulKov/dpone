"""Full desired-source membership bound to a v2 dbt release's identity."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.dbt_relation_writes import (
    DbtRelationWrite,
    require_distinct_logical_writes,
    selected_relation_writes,
    transfer_relation_write,
)
from dpone.contracts.dbt_release import dbt_release_authority_violation
from dpone.contracts.dbt_release_workload_binding import (
    DbtDevEvidenceReleaseError,
    ExpectedWorkflowDag,
    dbt_execution_from_pack,
    release_digest,
    release_mapping,
    release_object,
    require_dbt_workflow_dag,
)
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_runtime_release_binding import (
    DbtReleaseArtifactIndex,
)
from dpone.contracts.dbt_selected_graph_observation import observe_dbt_selected_graph
from dpone.contracts.dbt_selection_lock import DbtSelectionLock
from dpone.contracts.dbt_source_inventory import DbtProjectSource, DbtSourceInventory, DbtWorkflowSource
from dpone.contracts.dbt_workflow_graph_policy import evaluate_dbt_workflow_graph_ownership


def validate_dbt_source_inventory_binding(
    release: Mapping[str, object],
    inventory: DbtSourceInventory,
    *,
    expected_release_id: str,
) -> None:
    """Reject incomplete, foreign or orphan sources, even after outer resealing.

    This is a pure metadata contract, not a signature, artifact-byte, DAG-body
    or execution-pack verification. Full-tree readers must perform those checks
    before authorizing publication or constructing evidence expectations.
    """

    DbtSourcePlan.from_release(release, inventory, expected_release_id=expected_release_id)


def _bind_source_inventory(
    release: Mapping[str, object], inventory: DbtSourceInventory, expected_release_id: str
) -> DbtReleaseArtifactIndex:

    violation = dbt_release_authority_violation(release, expected_wire_contract=DBT_RUNTIME_WIRE_V2)
    if violation is not None:
        raise ValueError(violation)
    if (
        release.get("schema") != "dpone.release-set.v2"
        or release.get("release_id") != expected_release_id
        or release_id(release) != expected_release_id
    ):
        raise ValueError("dbt source release identity is invalid")
    provenance = release.get("provenance")
    if not isinstance(provenance, Mapping) or provenance.get("source_snapshot_sha256") != inventory.snapshot_sha256:
        raise ValueError("dbt source snapshot differs from release authority")
    index = DbtReleaseArtifactIndex.from_release(release)
    workflows = tuple(workflow for project in inventory.projects for workflow in project.workflows)
    workloads = index.workloads
    if {item_id for item_id in workloads if item_id.startswith("dbt__")} != {item.workload_id for item in workflows}:
        raise ValueError("dbt source workflow membership differs from release")
    if set(index.dags) != {item.dag_id for item in workflows}:
        raise ValueError("dbt source DAG membership differs from release")
    payloads = index.payloads
    if set(payloads) != {item_id for item in workflows for item_id in item.runtime_payload_ids}:
        raise ValueError("dbt source payload membership differs from release")
    for workflow in workflows:
        index.require_workload_trio(workflow.workload_id, workflow.runtime_payload_ids)
    for item_id, pack in workloads.items():
        if not item_id.startswith("dbt__") and "runtime_payload_ids" in pack:
            raise ValueError("transfer workloads cannot own dbt runtime trios")
    return index


@dataclass(frozen=True, slots=True)
class DbtWorkflowReleaseSource:
    """A workflow joined to verified project, execution and DAG membership."""

    project: DbtProjectSource
    source: DbtWorkflowSource
    execution: DbtExecutionPack
    dag: ExpectedWorkflowDag
    workload_pack_sha256: str


@dataclass(frozen=True, slots=True)
class DbtReleaseSources:
    """Whole-source result; not a signature or live execution proof."""

    release_id: str
    inventory: DbtSourceInventory
    workflows: tuple[DbtWorkflowReleaseSource, ...]
    required_workloads: tuple[tuple[str, str], ...]
    relation_writes: tuple[DbtRelationWrite, ...]


@dataclass(frozen=True, slots=True)
class DbtProjectSourceObservation:
    """Compact semantic observations without retaining source archives/manifests."""

    project: DbtProjectSource
    workflows: tuple[DbtWorkflowReleaseSource, ...]
    relation_writes: tuple[DbtRelationWrite, ...]


@dataclass(frozen=True, slots=True)
class DbtSourcePlan:
    """One metadata authority and pure project/release closure policy.

    The service must verify every supplied byte and framework pack fingerprint.
    These pure methods neither acquire files nor treat type annotations as proof
    that signature verification or live execution occurred.
    """

    release_id: str
    inventory: DbtSourceInventory
    artifacts: DbtReleaseArtifactIndex
    selection_fingerprints: tuple[str, ...]

    @classmethod
    def from_release(
        cls, release: Mapping[str, object], inventory: DbtSourceInventory, *, expected_release_id: str
    ) -> DbtSourcePlan:
        index = _bind_source_inventory(release, inventory, expected_release_id)
        provenance = release_mapping(release["provenance"], "release provenance")
        fingerprints = provenance["selection_fingerprints"]
        assert isinstance(fingerprints, list)  # Verified by release authority.
        return cls(expected_release_id, inventory, index, tuple(fingerprints))

    @staticmethod
    def decode_selection(payload: bytes) -> DbtSelectionLock:
        """Decode selection bytes already bounded and hash-verified by the reader."""

        return DbtSelectionLock.from_mapping(release_object(payload, "selection lock"))

    @staticmethod
    def execution_from_pack(source: DbtWorkflowSource, pack: Mapping[str, object]) -> DbtExecutionPack:
        """Bind an already fingerprint-verified pack to the V2 source trio.

        Framework identity must be checked by the reader before this method;
        interpreting a pack is not evidence that its fingerprint was verified.
        """

        return dbt_execution_from_pack(
            pack, expected_runtime_payload_ids=source.runtime_payload_ids, wire_contract=DBT_RUNTIME_WIRE_V2
        )

    def observe_project(
        self,
        project: DbtProjectSource,
        *,
        manifest: Mapping[str, object],
        bundled_project_name: object,
        executions: Mapping[str, tuple[DbtExecutionPack, DbtSelectionLock]],
        dags: Mapping[str, ExpectedWorkflowDag],
    ) -> DbtProjectSourceObservation:
        if project not in self.inventory.projects:
            raise DbtDevEvidenceReleaseError("project is outside the release source inventory")
        metadata = release_mapping(manifest.get("metadata"), "manifest metadata")
        if metadata.get("project_name") != project.project_name or bundled_project_name != project.project_name:
            raise DbtDevEvidenceReleaseError("manifest or bundled project name differs from source inventory")
        if set(executions) != {item.workflow_id for item in project.workflows}:
            raise DbtDevEvidenceReleaseError("project execution membership differs from source inventory")
        workflows = []
        writes: list[DbtRelationWrite] = []
        for source in project.workflows:
            execution, selection = executions[source.workflow_id]
            dag = require_dbt_workflow_dag(dags, source.workflow_id, source.workload_id)
            if (
                execution.workflow_id != source.workflow_id
                or dag.dag_id != source.dag_id
                or execution.project_bundle_sha256 != project.project_bundle_sha256
                or selection != execution.selection_lock
                or selection.manifest_sha256 != project.manifest_sha256
                or selection.toolchain_sha256 != project.toolchain_sha256
            ):
                raise DbtDevEvidenceReleaseError("dbt execution/DAG identity differs from source inventory")
            _validate_selection(execution, manifest)
            writes.extend(
                selected_relation_writes(project_path=project.project_path, execution=execution, manifest=manifest)
            )
            workflows.append(
                DbtWorkflowReleaseSource(
                    project,
                    source,
                    execution,
                    dag,
                    release_digest(self.artifacts.workloads[source.workload_id].get("sha256"), "pack sha256"),
                )
            )
        locks = {item.execution.workflow_id: item.execution.selection_lock for item in workflows}
        policy = evaluate_dbt_workflow_graph_ownership(
            publish_model_ids_by_workflow={key: lock.publish_model_unique_ids for key, lock in locks.items()},
            selected_graph_ids_by_workflow={key: lock.selected_graph_unique_ids for key, lock in locks.items()},
        )
        if not policy.passed:
            raise DbtDevEvidenceReleaseError("project workflows have overlapping model ownership")
        return DbtProjectSourceObservation(project, tuple(workflows), tuple(writes))

    def require_dags(self, dags: Mapping[str, ExpectedWorkflowDag]) -> None:
        expected = {source.workflow_id for project in self.inventory.projects for source in project.workflows}
        workloads = [key for dag in dags.values() for key in dag.workload_ids]
        if (
            set(dags) != expected
            or len(workloads) != len(set(workloads))
            or set(workloads) != set(self.artifacts.workloads)
        ):
            raise DbtDevEvidenceReleaseError("DAGs do not cover each release workload exactly once")

    def observe_transfer(
        self, workload_id: str, manifest: Mapping[str, object], *, owner: tuple[str, str]
    ) -> DbtRelationWrite:
        """Reduce one decoded manifest immediately; finish checks its DAG owner."""

        if manifest.get("name") != workload_id:
            raise DbtDevEvidenceReleaseError("transfer manifest identity is invalid")
        return transfer_relation_write(
            project_path=owner[0], workflow_id=owner[1], workload_id=workload_id, manifest=manifest
        )

    def finish(
        self,
        observations: tuple[DbtProjectSourceObservation, ...],
        *,
        transfer_writes: tuple[DbtRelationWrite, ...],
    ) -> DbtReleaseSources:
        if tuple(item.project for item in observations) != self.inventory.projects:
            raise DbtDevEvidenceReleaseError("project observations do not cover the ordered source inventory")
        workflows = tuple(item for project in observations for item in project.workflows)
        if tuple((item.project, item.source) for item in workflows) != tuple(
            (project, source) for project in self.inventory.projects for source in project.workflows
        ):
            raise DbtDevEvidenceReleaseError("workflow observations differ from the source inventory")
        self.require_dags({item.source.workflow_id: item.dag for item in workflows})
        owners = {
            workload_id: (item.project.project_path, item.source.workflow_id)
            for item in workflows
            for workload_id in item.dag.workload_ids
        }
        transfers = {key for key in self.artifacts.workloads if not key.startswith("dbt__")}
        if len(transfer_writes) != len(transfers) or {item.resource_id for item in transfer_writes} != transfers:
            raise DbtDevEvidenceReleaseError("transfer manifests do not cover the release workload inventory")
        writes = [item for project in observations for item in project.relation_writes]
        for item in transfer_writes:
            if item.kind != "transfer" or (item.project_path, item.workflow_id) != owners[item.resource_id]:
                raise DbtDevEvidenceReleaseError("transfer source owner differs from its workflow DAG")
            writes.append(item)
        require_distinct_logical_writes(writes)
        if self.selection_fingerprints != tuple(
            sorted({item.execution.selection_lock.selection_sha256 for item in workflows})
        ):
            raise DbtDevEvidenceReleaseError("release selection evidence differs from verified workflows")
        return DbtReleaseSources(
            self.release_id,
            self.inventory,
            workflows,
            tuple(
                (key, release_digest(row.get("sha256"), "pack sha256"))
                for key, row in sorted(self.artifacts.workloads.items())
            ),
            tuple(writes),
        )


def _validate_selection(execution: DbtExecutionPack, manifest: Mapping[str, object]) -> None:
    lock = execution.selection_lock
    try:
        observation = observe_dbt_selected_graph(
            manifest, lock=lock, logical_target=(execution.profile.database, execution.profile.schema)
        )
        # Runtime preflight additionally observes actual dbt ls. This only
        # compares the locked source graph, never certifies database behavior.
        observation.require_matches(lock, lock.selected_graph_unique_ids)
    except DbtPublishingError as exc:
        raise DbtDevEvidenceReleaseError("selection lock differs from admitted manifest semantics") from exc


__all__ = ["DbtSourcePlan", "DbtReleaseSources", "DbtWorkflowReleaseSource", "validate_dbt_source_inventory_binding"]
