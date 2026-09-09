"""Complete logical write coordinates, without guessing physical SQL identity.

Literal collisions can be rejected offline. Different aliases, default database
bindings and identifier spellings still require environment-bound verification.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.contracts.dbt_contract_validation import DbtPublishingError

if TYPE_CHECKING:
    from dpone.contracts.dbt_execution_pack import DbtExecutionPack


@dataclass(frozen=True, slots=True)
class DbtRelationWrite:
    """One model, unit-test or transfer write, retaining its owner and purpose."""

    project_path: str
    workflow_id: str
    resource_id: str
    kind: Literal["model", "unit_test", "transfer"]
    connector: str
    connection_ref: str
    database: str | None
    schema: str
    relation: str
    role: Literal["target", "intermediate", "backup", "helper"] = "target"

    def __post_init__(self) -> None:
        for name in ("project_path", "workflow_id", "resource_id", "connector", "connection_ref", "schema", "relation"):
            _text(getattr(self, name), project=self.project_path)
        if self.database is not None:
            _text(self.database, project=self.project_path)
        if self.kind not in {"model", "unit_test", "transfer"}:
            raise _invalid(self.project_path)
        if self.role not in {"target", "intermediate", "backup", "helper"} or (
            self.kind == "transfer" and self.role != "target"
        ):
            raise _invalid(self.project_path)
        object.__setattr__(self, "connector", canonical_endpoint_type(self.connector))

    @property
    def owner(self) -> str:
        owner = f"{self.project_path}:{self.workflow_id}/{self.resource_id}"
        return owner if self.role == "target" else f"{owner} ({self.role})"

    @property
    def logical_key(self) -> tuple[str, str, str | None, str, str]:
        """Exact declared coordinates, not a physical database identity."""

        return self.connector, self.connection_ref, self.database, self.schema, self.relation


def selected_relation_writes(
    *, project_path: str, execution: DbtExecutionPack, manifest: Mapping[str, object]
) -> tuple[DbtRelationWrite, ...]:
    """Inventory admitted SQL Server model and unit-test relation mutations.

    Callers must first validate the pinned graph/macro authority. Its table,
    view and incremental materializations all drop preexisting intermediate and
    backup relations, even when a subsequent branch avoids the rename itself.
    These are exact reserved coordinates, not simulated collation/SQL behavior.
    """

    nodes = _mapping(manifest.get("nodes"), project_path)
    writes = []
    for unique_id in execution.selection_lock.selected_graph_unique_ids:
        if unique_id.startswith("unit_test."):
            unit = _mapping(_mapping(manifest.get("unit_tests"), project_path).get(unique_id), project_path)
            dependencies = _mapping(unit.get("depends_on"), project_path).get("nodes")
            if (
                not isinstance(dependencies, list)
                or not dependencies
                or not isinstance(dependencies[0], str)
                or not dependencies[0].startswith("model.")
                or dependencies[0] not in execution.selection_lock.selected_graph_unique_ids
            ):
                raise _invalid(project_path)
            tested = _mapping(nodes.get(dependencies[0]), project_path)
            alias = _text(unit.get("name"), project=project_path)
            version = tested.get("version")
            if version is not None:
                if type(version) not in {str, int, float}:
                    raise _invalid(project_path)
                alias += f"_v{version}"
            target = _selected_target(
                project_path,
                execution,
                unique_id,
                tested,
                kind="unit_test",
                relation=alias + "__dbt_tmp",
                role="intermediate",
            )
            writes.extend((target, replace(target, relation=target.relation + "__dbt_tmp_vw", role="helper")))
            continue
        if unique_id.startswith("test."):
            continue  # Admitted data tests forbid storing failures; unit tests above do write.
        if not unique_id.startswith("model."):
            raise _invalid(project_path)
        node = _mapping(nodes.get(unique_id), project_path)
        config = _mapping(node.get("config"), project_path)
        if node.get("resource_type") != "model" or config.get("materialized") not in {"table", "view", "incremental"}:
            raise _invalid(project_path)
        target = _selected_target(
            project_path,
            execution,
            unique_id,
            node,
            kind="model",
            relation=_text(node.get("alias") or node.get("name"), project=project_path),
        )
        writes.extend(
            (
                target,
                replace(target, relation=target.relation + "__dbt_tmp", role="intermediate"),
                replace(target, relation=target.relation + "__dbt_backup", role="backup"),
            )
        )
        if config["materialized"] in {"table", "incremental"}:
            writes.append(replace(target, relation=target.relation + "__dbt_tmp__dbt_tmp_vw", role="helper"))
        if config["materialized"] == "incremental":
            writes.append(replace(target, relation=target.relation + "__dbt_tmp_vw", role="helper"))
    if not writes:
        raise _invalid(project_path)
    return tuple(writes)


def _selected_target(
    project: str,
    execution: DbtExecutionPack,
    unique_id: str,
    node: Mapping[str, object],
    *,
    kind: Literal["model", "unit_test"],
    relation: str,
    role: Literal["target", "intermediate"] = "target",
) -> DbtRelationWrite:
    target = DbtRelationWrite(
        project_path=project,
        workflow_id=execution.workflow_id,
        resource_id=unique_id,
        kind=kind,
        connector=execution.profile.adapter_type,
        connection_ref=execution.profile.connection_ref,
        database=_text(node.get("database"), project=project),
        schema=_text(node.get("schema"), project=project),
        relation=relation,
        role=role,
    )
    if target.connector != "mssql":
        raise _invalid(project)  # No inferred footprint for an uncertified adapter.
    return target


def transfer_relation_write(
    *, project_path: str, workflow_id: str, workload_id: str, manifest: Mapping[str, object]
) -> DbtRelationWrite:
    """Read the generated transfer sink; defaults remain explicitly unresolved."""

    sink = _mapping(manifest.get("sink"), project_path)
    table = _mapping(sink.get("table"), project_path)
    database = table.get("database")
    return DbtRelationWrite(
        project_path=project_path,
        workflow_id=workflow_id,
        resource_id=workload_id,
        kind="transfer",
        connector=_text(sink.get("type"), project=project_path),
        connection_ref=_text(sink.get("connection_ref"), project=project_path),
        database=None if database is None else _text(database, project=project_path),
        schema=_text(table.get("schema"), project=project_path),
        relation=_text(table.get("name"), project=project_path),
    )


def require_distinct_logical_writes(writes: Sequence[DbtRelationWrite]) -> None:
    """Reject duplicate writers without claiming different keys are physically disjoint."""

    owners: dict[tuple[str, str, str | None, str, str], DbtRelationWrite] = {}
    for row in sorted(
        writes, key=lambda item: (item.project_path, item.workflow_id, item.kind, item.resource_id, item.role)
    ):
        previous = owners.get(row.logical_key)
        if previous is not None:
            raise DbtPublishingError(
                "DPONE_DBT_WORKSPACE_TARGET_COLLISION",
                f"Logical relation collision between {previous.owner} and {row.owner}",
                path=row.project_path,
                remediation="Give each materialized relation one writer; review model aliases and transfer targets. Do not rename deployed DAG IDs or bypass physical preflight.",
            )
        owners[row.logical_key] = row


def _mapping(value: object, project: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _invalid(project)
    return value


def _text(value: object, *, project: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise _invalid(project)
    return value


def _invalid(project: str) -> DbtPublishingError:
    return DbtPublishingError(
        "DPONE_DBT_WORKSPACE_TARGET_INVALID",
        "Selected relation write coordinates are incomplete or unsupported",
        path=project,
        remediation="Regenerate the canonical manifest and correct the generated transfer sink through project-local publishing policy.",
    )
