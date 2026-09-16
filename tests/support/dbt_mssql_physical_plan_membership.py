"""Synthetic retained source fixtures and explicitly pure comparison inputs."""

import json
from dataclasses import replace

from dpone.adapters.dbt_mssql_physical_plan_membership import MssqlPhysicalPlanMembershipReader
from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.dbt_mssql_physical import (
    AbsentPredecessor,
    PhysicalFilegroup,
    PhysicalModelPlan,
    PhysicalModelSpec,
    PhysicalPlanSet,
    PhysicalRelation,
)
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.manifest.confined_files import read_confined_file
from tests.test_dbt_mssql_physical_catalog_policy import _claim, _fixture

D = "sha256:" + "a" * 64
G = "11111111-1111-4111-8111-111111111111"


def retained_fixture(tmp_path, monkeypatch, *, malformed_schema=False):
    from copy import deepcopy

    from tests import test_dbt_release_source_reader as source
    from tests.test_dbt_sqlserver_graph_policy import _model as minimal_model

    prototype = next(node for node in source._AUTHORITY_MANIFEST["nodes"].values() if node["resource_type"] == "model")

    def model():
        node = deepcopy(prototype)
        minimal = minimal_model()
        config = {**node["config"], **minimal.pop("config")}
        node.update(minimal, config=config)
        if malformed_schema:
            # Description is schema-constrained but does not affect graph policy.
            node["description"] = 7
        return node

    monkeypatch.setattr(source, "_model", model)
    return _fixture(
        tmp_path,
        monkeypatch,
        policy_change=lambda profile: profile["native_execution"].update(physical_filegroup={"name": "PRIMARY"}),
    )


def reader(verifier, fixture, *, read_file=read_confined_file, maximum=8 * 1024 * 1024):
    return MssqlPhysicalPlanMembershipReader(
        verifier=verifier,
        documents=NativeProjectDocumentReader(read_file=read_confined_file, max_policy_bytes=1024 * 1024),
        read_file=read_file,
        release_root=fixture.release_root,
        max_release_bytes=maximum,
    )


def inputs(verifier, fixture):
    claim = replace(_claim(verifier, fixture), local_schema="dpone_physical")
    original = verifier.resolve(fixture.refs)
    owner = next(item for item in original.sources.workflows if item.source.workflow_id == "alpha")
    release = json.loads((fixture.release_root / fixture.refs.release.locator).read_bytes())
    descriptor = next(
        item for item in release["artifacts"]["runtime_payloads"] if item["id"] == owner.source.runtime_payload_ids[1]
    )
    manifest = json.loads((fixture.release_root / descriptor["path"]).read_bytes())
    selection = owner.execution.selection_lock
    models = []
    for key in selection.selected_graph_unique_ids:
        if not key.startswith("model."):
            continue
        node = manifest["nodes"][key]
        spec = PhysicalModelSpec(
            key,
            selection.graph_contract_sha256,
            PhysicalRelation(node["database"], node["schema"], node.get("alias", node["name"])),
            (MssqlCatalogColumn("id", "int", False),),
            "rowstore_none",
            PhysicalFilegroup(1, "PRIMARY"),
            claim.trusted_profile.reference,
        )
        models.append(PhysicalModelPlan(G, spec, AbsentPredecessor()))
    plan = PhysicalPlanSet(
        G,
        claim.registration_id,
        DbtWorkspaceAttemptRequest.build(activation_id=G, attempt_id=D, workflow_id="alpha", write_subjects=(D,)),
        DbtWorkspaceGuardEpoch("guard", 1),
        claim.trusted_profile.reference,
        claim.model_database,
        tuple(models),
    )
    profile = json.loads(original.policy_document)["profiles"]["local"]
    return claim, plan, manifest, selection, profile


def pure_managed_inputs(verifier, fixture):
    """Local mapping edits are ONLY for the pure comparator, not source admission."""
    claim, plan, manifest, selection, profile = inputs(verifier, fixture)
    for model in plan.models:
        node = manifest["nodes"][model.spec.model_unique_id]
        node["alias"] = model.spec.relation.table
        node["config"].update(
            materialized="dpone_managed_table",
            enabled=True,
            contract={"enforced": True},
            meta={"dpone": {"publish": {"model_storage": "rowstore_none"}}},
        )
        node["columns"] = {"id": {"name": "id", "data_type": "int", "constraints": [{"type": "not_null"}]}}
    return claim, plan, manifest, selection, profile


def with_columns(plan, columns):
    return replace(
        plan, models=tuple(replace(model, spec=replace(model.spec, columns=columns)) for model in plan.models)
    )
