"""Whole-tree source verification with real bundles/packs, without live dbt."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_execution_pack import DbtInvocationTarget
from dpone.contracts.dbt_publishing import DbtExecutionPack, DbtSelectionLock
from dpone.contracts.dbt_release import dbt_selection_fingerprint
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V2,
    dbt_runtime_payload_reference,
    dbt_runtime_payload_trio,
)
from dpone.contracts.dbt_source_inventory import DbtProjectSource, DbtSourceInventory, DbtWorkflowSource
from dpone.contracts.dbt_sqlserver_graph_policy_contract import dbt_sqlserver_graph_contract_sha256
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder
from dpone.services.dbt_release_assets import canonical_schema_descriptors, canonical_schema_files
from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
from dpone.services.dbt_release_workflow_reader import DbtDevEvidenceReleaseError
from tests.test_airflow_runtime_init_fetch_cli import _dbt_runtime_fixture
from tests.test_dbt_source_inventory_binding import _release
from tests.test_dbt_sqlserver_graph_policy import _AUTHORITY_MANIFEST, _model


def _json(value) -> bytes:
    return json.dumps(value, sort_keys=True).encode()


def _write(root: Path, path: str, payload: bytes) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)


def _tree(
    tmp_path: Path,
    *,
    same_project: bool = False,
    bad_graph: bool = False,
    unsafe_graph: bool = False,
    result_drift: bool = False,
    selection_padding: int = 0,
    legacy_pack: bool = False,
    logical_collision: bool = False,
):
    root = tmp_path / "compiled"
    sources, locks, packs, dags, bodies = [], [], {}, {}, {}
    for name in ("alpha", "beta"):
        project_name = "alpha" if same_project else name
        project = tmp_path / name
        project.mkdir()
        (project / "dbt_project.yml").write_text(f"name: {project_name}\n", encoding="utf-8")
        (project / "models").mkdir()
        (project / "models/orders.sql").write_text("select 1 as id\n", encoding="utf-8")
        fixture = _dbt_runtime_fixture(project)
        manifest = deepcopy(_AUTHORITY_MANIFEST)
        manifest["metadata"]["project_name"] = project_name
        # A dependency can expose the same local node ID in different projects.
        node = _model()
        target_schema = "shared" if same_project or logical_collision else name
        node.update(fqn=["analytics", "orders"], database="analytics", schema=target_schema)
        if unsafe_graph:
            node["config"]["pre-hook"] = ["delete from unrelated"]
        manifest["nodes"] = {"model.analytics.orders": node}
        selected = ("model.analytics.orders",)
        if result_drift:
            another = deepcopy(node)
            another.update(unique_id="model.analytics.other", name="other", fqn=["analytics", "other"])
            manifest["nodes"]["model.analytics.other"] = another
            selected = (*selected, "model.analytics.other")
        manifest_bytes = _json(manifest)
        values = json.loads(fixture.selection_lock)
        values.pop("schema")
        values.pop("selection_sha256")
        values["manifest_sha256"] = sha256_bytes(manifest_bytes)
        if not bad_graph:
            values["graph_contract_sha256"] = dbt_sqlserver_graph_contract_sha256(manifest, selected)
        for key in (
            "selectors",
            "selected_graph_unique_ids",
            "expected_run_result_unique_ids",
            "publish_model_unique_ids",
        ):
            values[key] = tuple(values[key])
        values["selected_graph_unique_ids"] = selected
        lock = DbtSelectionLock.build(**values)
        locks.append(lock.selection_sha256)
        old = DbtExecutionPack.from_mapping(json.loads(fixture.execution_pack))
        build_pack = DbtExecutionPack.build if legacy_pack else DbtExecutionPack.build_v2
        execution = build_pack(
            **({} if legacy_pack else {"invocation_target": DbtInvocationTarget("analytics", "base")}),
            workflow_id=name,
            project_bundle_sha256=old.project_bundle_sha256,
            project_subdir=old.project_subdir,
            target_path=old.target_path,
            profile=replace(old.profile, schema=target_schema),
            selection_lock=lock,
            invocation_context=old.invocation_context,
            adapter_runtime=old.adapter_runtime,
            adapter_policy=old.adapter_policy,
            dbt_warning_policy=old.dbt_warning_policy,
            timeout_seconds=old.timeout_seconds,
        )
        selection = _json(lock.to_dict()) + b" " * selection_padding
        ids = dbt_runtime_payload_trio(
            workflow_id=name,
            project_sha256=old.project_bundle_sha256,
            manifest_sha256=sha256_bytes(manifest_bytes),
            selection_lock_payload=selection,
            wire_contract=DBT_RUNTIME_WIRE_V2,
        )
        workflow = DbtWorkflowSource(
            name, f"DAG__{name}__orders__publish", f"dbt__{name}", ids, sha256_bytes(selection)
        )
        sources.append(
            DbtProjectSource(
                f"dbt/{project_name}",
                project_name,
                old.project_bundle_sha256,
                sha256_bytes(manifest_bytes),
                lock.toolchain_sha256,
                (workflow,),
            )
        )
        for item_id, body in zip(ids, (fixture.project_archive, manifest_bytes, selection), strict=True):
            bodies[item_id] = body
        packs[workflow.workload_id] = DbtAirflowExecutionPackBuilder().build(
            workflow_id=name,
            execution_pack=execution,
            runtime_payload_ids=ids,
            xcom_sidecar_image="example.invalid/sidecar@sha256:" + "a" * 64,
            pool="dbt",
        )
        dags[workflow.dag_id] = {
            "dag_id": workflow.dag_id,
            "source": {"workflow": name},
            "nodes": [{"workload_id": workflow.workload_id}],
            "workflow_outcome": {
                "schema": "dpone.dbt-workflow-outcome.v1",
                "workflow_id": name,
                "task_id": "outcome",
                "expected_terminal_task_ids": ["dbt_runtime"],
            },
        }
    if same_project:
        sources = [replace(sources[0], workflows=(*sources[0].workflows, *sources[1].workflows))]
    inventory = DbtSourceInventory.build(sources)
    release = _release(inventory)
    schemas = canonical_schema_files()
    release["artifacts"]["canonical_schemas"] = canonical_schema_descriptors(schemas)
    for path, body in schemas.items():
        _write(root, path, body)
    for descriptor in release["artifacts"]["runtime_payloads"]:
        reference = dbt_runtime_payload_reference(descriptor["id"], wire_contract=DBT_RUNTIME_WIRE_V2)
        descriptor.update(reference.descriptor(bodies[descriptor["id"]]))
        _write(root, descriptor["path"], bodies[descriptor["id"]])
    for section, items in (("workload_packs", packs), ("dag_specs", dags)):
        for descriptor in release["artifacts"][section]:
            body = _json(items[descriptor["id"]])
            descriptor.update(sha256=sha256_bytes(body), bytes=len(body))
            if section == "workload_packs":
                descriptor["pack_fingerprint"] = items[descriptor["id"]]["pack_fingerprint"]
            _write(root, descriptor["path"], body)
    release["provenance"]["selection_fingerprints"] = sorted(set(locks))
    _write(root, "_dbt/dbt-source-snapshot.json", _json(inventory.to_dict()))
    _seal(root, release)
    return root, release, inventory


def test_workspace_source_reader_rejects_consistently_hashed_legacy_pack(tmp_path):
    root, release, _ = _tree(tmp_path, legacy_pack=True)
    with pytest.raises(DbtDevEvidenceReleaseError, match="wire version"):
        _read(root, release)


def _seal(root: Path, release: dict) -> None:
    release["selection_fingerprint"] = dbt_selection_fingerprint(**release["provenance"])
    release["release_id"] = release_id(release)
    _write(root, "release-set.json", _json(release))


def _read(root: Path, release: dict):
    return DbtReleaseSourceReader(
        bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=read_confined_file
    ).read(
        root,
        expected_release_id=release["release_id"],
    )


def test_reads_both_project_sources_and_their_execution_identities(tmp_path: Path) -> None:
    root, release, inventory = _tree(tmp_path)
    result = _read(root, release)
    assert result.inventory == inventory
    assert tuple(item.execution.workflow_id for item in result.workflows) == ("alpha", "beta")
    assert {item.project.project_name for item in result.workflows} == {"alpha", "beta"}
    assert all(
        item.execution.selection_lock.publish_model_unique_ids == ("model.analytics.orders",)
        for item in result.workflows
    )
    assert len(result.relation_writes) == 8
    assert {row.schema for row in result.relation_writes} == {"alpha", "beta"}


def test_consistently_hashed_release_cannot_hide_cross_project_model_writers(tmp_path):
    from dpone.contracts.dbt_contract_validation import DbtPublishingError

    root, release, _ = _tree(tmp_path, logical_collision=True)
    with pytest.raises(DbtDevEvidenceReleaseError) as failure:
        _read(root, release)
    assert isinstance(failure.value.__cause__, DbtPublishingError)
    assert failure.value.__cause__.code == "DPONE_DBT_WORKSPACE_TARGET_COLLISION"


def test_evidence_expectations_include_both_projects(tmp_path: Path) -> None:
    from dpone.app.dbt_promotion_composition import build_dbt_expected_release_loader

    root, release, inventory = _tree(tmp_path)
    result = build_dbt_expected_release_loader()(root, release["release_id"])
    assert set(result.dbt_workflows) == {"alpha", "beta"}
    assert set(result.required_workloads) == {"dbt__alpha", "dbt__beta"}
    assert result.dbt_workflows["beta"].project_bundle_sha256 == inventory.projects[1].project_bundle_sha256
    assert result.dbt_workflows["alpha"].manifest_sha256 != result.dbt_workflows["beta"].manifest_sha256


def test_campaign_request_cannot_omit_project_b(tmp_path: Path) -> None:
    from dpone.app.dbt_promotion_composition import build_dbt_expected_release_loader
    from dpone.services.dbt_dev_evidence_request import DbtDevEvidenceRequest, DbtDevEvidenceRequestError

    root, release, inventory = _tree(tmp_path)
    arguments = dict(
        compiled_root=root,
        release_id=release["release_id"],
        deployment_id=sha256_bytes(b"dev"),
        producer_repository="example/repo",
        producer_workflow=".github/workflows/accept.yml",
        source_commit="a" * 40,
        orchestration_run_id="123",
        orchestration_run_attempt=1,
        release_loader=build_dbt_expected_release_loader(),
    )
    request = DbtDevEvidenceRequest.build(**arguments)
    assert tuple(item.workflow_id for item in request.workflows) == ("alpha", "beta")
    reference = dbt_runtime_payload_reference(
        inventory.projects[1].workflows[0].runtime_payload_ids[0], wire_contract=DBT_RUNTIME_WIRE_V2
    )
    (root / reference.path).unlink()
    with pytest.raises(DbtDevEvidenceRequestError):
        DbtDevEvidenceRequest.build(**arguments)


def test_cli_prepares_complete_workspace_campaign_without_reader_mocks(tmp_path: Path, capsys) -> None:
    from dpone.cli.main import main

    root, release, _ = _tree(tmp_path)
    with pytest.raises(SystemExit) as caught:
        main(
            [
                "dbt",
                "prepare-dev-evidence-request",
                "--compiled-root",
                str(root),
                "--expected-release-id",
                release["release_id"],
                "--expected-deployment-id",
                sha256_bytes(b"dev"),
                "--producer-repository",
                "example/repo",
                "--producer-workflow",
                ".github/workflows/accept.yml",
                "--source-commit",
                "a" * 40,
                "--orchestration-run-id",
                "123",
                "--orchestration-run-attempt",
                "1",
                "--format",
                "json",
            ]
        )
    assert caught.value.code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert [item["workflow_id"] for item in json.loads(captured.out)["workflows"]] == ["alpha", "beta"]


def test_injected_reader_covers_workload_and_dag_bytes_too(tmp_path: Path) -> None:
    from dpone.manifest.confined_files import read_confined_file

    root, release, _ = _tree(tmp_path)
    observed = set()

    def reader(root, relative_path, *, max_bytes):
        observed.add(relative_path)
        return read_confined_file(root, relative_path, max_bytes=max_bytes)

    DbtReleaseSourceReader(bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=reader).read(
        root, expected_release_id=release["release_id"]
    )
    assert {
        item["path"]
        for section in ("workload_packs", "dag_specs", "runtime_payloads", "canonical_schemas")
        for item in release["artifacts"][section]
    } <= observed


def test_whole_source_reader_validates_each_runtime_descriptor_once(tmp_path, monkeypatch):
    import dpone.contracts.dbt_runtime_release_binding as binding

    root, release, _ = _tree(tmp_path)
    calls = []
    validate = binding.validate_dbt_runtime_payload_descriptor

    def counted(row, **kwargs):
        calls.append(row["id"])
        return validate(row, **kwargs)

    monkeypatch.setattr(binding, "validate_dbt_runtime_payload_descriptor", counted)
    _read(root, release)
    assert sorted(calls) == sorted(item["id"] for item in release["artifacts"]["runtime_payloads"])


def test_semantic_reread_rechecks_bytes_after_canonical_tree_verification(tmp_path):
    root, release, _ = _tree(tmp_path)
    path = release["artifacts"]["runtime_payloads"][0]["path"]
    reads = 0

    def reader(root, relative_path, *, max_bytes):
        nonlocal reads
        body = read_confined_file(root, relative_path, max_bytes=max_bytes)
        if relative_path == path:
            reads += 1
            if reads == 2:
                # Preserve length: previous metadata cannot bless substituted bytes.
                return bytes([body[0] ^ 1]) + body[1:]
        return body

    with pytest.raises(DbtDevEvidenceReleaseError):
        DbtReleaseSourceReader(bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=reader).read(
            root, expected_release_id=release["release_id"]
        )
    assert reads == 2


@pytest.mark.parametrize("extra", ["orphan.sql", "_dbt/extra.json", "dbt__alpha/airflow-pack.json", "empty-directory"])
def test_fresh_integrity_subject_cannot_bless_unreferenced_files_or_directories(tmp_path: Path, extra: str) -> None:
    from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService

    root, release, _ = _tree(tmp_path)
    target = root / extra
    target.parent.mkdir(parents=True, exist_ok=True)
    if extra == "empty-directory":
        target.mkdir()
    else:
        target.write_bytes(b"not a release artifact")
    DbtReleaseIntegrityService().write(root)
    # The independent checksum inventory is green, but source closure must fail.
    DbtReleaseIntegrityService().verify(root)
    with pytest.raises(DbtDevEvidenceReleaseError):
        DbtReleaseSourceReader(
            bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=read_confined_file
        ).read(root, expected_release_id=release["release_id"])


@pytest.mark.parametrize("action", ["missing", "tampered", "symlink"])
def test_canonical_schema_bytes_are_not_exempt_from_source_closure(tmp_path: Path, action: str) -> None:
    root, release, _ = _tree(tmp_path)
    path = root / release["artifacts"]["canonical_schemas"][0]["path"]
    path.unlink()
    if action == "tampered":
        path.write_bytes(b"{}")
    elif action == "symlink":
        path.symlink_to(root / "release-set.json")
    with pytest.raises(DbtDevEvidenceReleaseError):
        DbtReleaseSourceReader(
            bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=read_confined_file
        ).read(root, expected_release_id=release["release_id"])


@pytest.mark.parametrize("digest", [None, "bad", "sha256:" + "A" * 64])
def test_malformed_schema_digest_is_a_domain_failure_not_key_error(tmp_path: Path, digest) -> None:
    root, release, _ = _tree(tmp_path)
    descriptor = release["artifacts"]["canonical_schemas"][0]
    if digest is None:
        descriptor.pop("sha256")
    else:
        descriptor["sha256"] = digest
    _seal(root, release)
    with pytest.raises(DbtDevEvidenceReleaseError):
        DbtReleaseSourceReader(
            bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=read_confined_file
        ).read(root, expected_release_id=release["release_id"])


@pytest.mark.parametrize("section", ["dag_specs", "workload_packs"])
def test_oversized_pack_or_dag_is_rejected_before_any_byte_acquisition(tmp_path: Path, section: str) -> None:
    from dpone.manifest.confined_files import read_confined_file

    root, release, _ = _tree(tmp_path)
    descriptor = release["artifacts"][section][0]
    descriptor["bytes"] = 8 * 1024 * 1024 + 1
    _seal(root, release)
    acquired = []

    def reader(root, relative_path, *, max_bytes):
        acquired.append(relative_path)
        assert relative_path != descriptor["path"], "oversized artifact must not be acquired"
        return read_confined_file(root, relative_path, max_bytes=max_bytes)

    with pytest.raises(DbtDevEvidenceReleaseError):
        DbtReleaseSourceReader(bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=reader).read(
            root, expected_release_id=release["release_id"]
        )
    assert descriptor["path"] not in acquired


@pytest.mark.parametrize("kind", ["dbt_project_bundle", "dbt_manifest", "dbt_selection_lock"])
@pytest.mark.parametrize("action", ["missing", "tampered", "symlink"])
def test_missing_or_invalid_project_b_source_blocks_whole_release(tmp_path: Path, kind: str, action: str) -> None:
    root, release, inventory = _tree(tmp_path)
    ids = inventory.projects[1].workflows[0].runtime_payload_ids
    descriptor = next(
        item for item in release["artifacts"]["runtime_payloads"] if item["id"] in ids and item["kind"] == kind
    )
    path = root / descriptor["path"]
    if action == "tampered":
        path.write_bytes(path.read_bytes() + b"tampered")
    else:
        content = path.read_bytes()
        path.unlink()
        if action == "symlink":
            foreign = tmp_path / "foreign"
            foreign.write_bytes(content)
            path.symlink_to(foreign)
    with pytest.raises(DbtDevEvidenceReleaseError):
        _read(root, release)


def test_resealed_project_name_cannot_disagree_with_real_source(tmp_path: Path) -> None:
    root, release, inventory = _tree(tmp_path)
    inventory = DbtSourceInventory.build(
        (replace(inventory.projects[0], project_name="impostor"), inventory.projects[1])
    )
    _write(root, "_dbt/dbt-source-snapshot.json", _json(inventory.to_dict()))
    release["provenance"]["source_snapshot_sha256"] = inventory.snapshot_sha256
    _seal(root, release)
    with pytest.raises(DbtDevEvidenceReleaseError):
        _read(root, release)


@pytest.mark.parametrize("option", ["same_project", "bad_graph", "unsafe_graph", "result_drift"])
def test_resealed_workflow_selection_must_satisfy_project_graph_policy(tmp_path: Path, option: str) -> None:
    root, release, _ = _tree(tmp_path, **{option: True})
    with pytest.raises(DbtDevEvidenceReleaseError):
        _read(root, release)


def test_selection_json_retains_narrow_evidence_reader_limit(tmp_path: Path) -> None:
    root, release, _ = _tree(tmp_path, selection_padding=1024 * 1024)
    with pytest.raises(DbtDevEvidenceReleaseError):
        _read(root, release)


@pytest.mark.parametrize("mutation", ["pack_ids", "dag_workflow", "selection_fingerprints"])
def test_consistently_resealed_semantic_drift_is_rejected(tmp_path: Path, mutation: str) -> None:
    root, release, _ = _tree(tmp_path)
    if mutation == "selection_fingerprints":
        release["provenance"]["selection_fingerprints"] = [sha256_bytes(b"other")]
    else:
        section = "workload_packs" if mutation == "pack_ids" else "dag_specs"
        descriptor = release["artifacts"][section][0]
        value = json.loads((root / descriptor["path"]).read_bytes())
        if mutation == "pack_ids":
            value["runtime_payload_ids"].reverse()
            value["pack_fingerprint"] = compute_pack_fingerprint(value)
            descriptor["pack_fingerprint"] = value["pack_fingerprint"]
        else:
            value["source"]["workflow"] = "other"
        body = _json(value)
        descriptor.update(sha256=sha256_bytes(body), bytes=len(body))
        _write(root, descriptor["path"], body)
    _seal(root, release)
    with pytest.raises(DbtDevEvidenceReleaseError):
        _read(root, release)
