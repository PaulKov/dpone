"""Real source rejection and pure comparisons; no managed qualification claim."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.adapters.dbt_mssql_physical_plan_membership import MssqlPhysicalPlanMembershipReader
from dpone.contracts.dbt_mssql_physical_plan_membership import (
    PhysicalPlanMembershipError,
    require_model_plan_membership,
)
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.manifest.confined_files import read_confined_file
from tests.support.dbt_mssql_physical_plan_membership import (
    inputs,
    pure_managed_inputs,
    reader,
    retained_fixture,
    with_columns,
)
from tests.test_native_original_verification import _native_verifier


def test_reader_rejects_proof_callback_and_resolved_dto(tmp_path):
    with pytest.raises(ValueError, match="concrete"):
        MssqlPhysicalPlanMembershipReader(
            verifier=lambda refs: None,
            documents=object(),
            read_file=lambda *args, **kwargs: b"",
            release_root=tmp_path,
            max_release_bytes=1024,
        )


@pytest.fixture
def retained(tmp_path, monkeypatch):
    fixture = retained_fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        yield fixture, verifier, inputs(verifier, fixture)


@pytest.fixture(scope="module")
def pure_source(tmp_path_factory):
    with pytest.MonkeyPatch.context() as patch:
        fixture = retained_fixture(tmp_path_factory.mktemp("membership-pure"), patch)
        with _native_verifier(fixture, []) as verifier:
            yield pure_managed_inputs(verifier, fixture)


def compare(data):
    import json
    from pathlib import Path

    from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader

    claim, plan, manifest, selection, profile = data
    # Pure comparison inputs are not passed off as schema/source admission.
    artifact, _issues = DbtArtifactReader().read_payload(json.dumps(manifest).encode(), path=Path("pure.json"))
    assert artifact is not None
    require_model_plan_membership(
        manifest,
        selection=selection,
        plan_set=plan,
        profile=profile,
        limits=claim.limits,
        logical_target=("analytics", "alpha"),
        resource_bounds=claim.trusted_profile.reference,
        declared_models=artifact.models,
    )


def test_real_indexed_legacy_source_is_not_managed(retained):
    fixture, verifier, (claim, plan, *_rest) = retained
    calls = []

    def read(root, path, *, max_bytes):
        calls.append((path, max_bytes))
        return read_confined_file(root, path, max_bytes=max_bytes)

    with pytest.raises(PhysicalPlanMembershipError, match="MANAGED_SOURCE_REQUIRED"):
        reader(verifier, fixture, read_file=read).require_plan_membership(
            fixture.refs, registration=claim, plan_set=plan
        )
    assert len(calls) == 3
    assert calls[0][0] == fixture.refs.release.locator
    assert all(bound > 0 for _, bound in calls)
    assert all(not resolver.calls for resolver in fixture.resolver_factory.resolvers)


@pytest.mark.parametrize("artifact", ["release", "manifest", "selection"])
def test_real_reacquisition_rejects_changed_bytes(retained, artifact):
    fixture, verifier, (claim, plan, *_rest) = retained
    count = 0
    target = {"release": 1, "manifest": 2, "selection": 3}[artifact]

    def read(root, path, *, max_bytes):
        nonlocal count
        count += 1
        body = read_confined_file(root, path, max_bytes=max_bytes)
        return body + b" " if count == target else body

    with pytest.raises(ValueError):
        reader(verifier, fixture, read_file=read).require_plan_membership(
            fixture.refs, registration=claim, plan_set=plan
        )
    assert count == target


def test_real_release_byte_limit_is_enforced(retained):
    from dpone.manifest.confined_files import ConfinedFileError

    fixture, verifier, (claim, plan, *_rest) = retained
    with pytest.raises(ConfinedFileError, match="byte limit"):
        reader(verifier, fixture, maximum=1).require_plan_membership(fixture.refs, registration=claim, plan_set=plan)


def test_registered_policy_substitution_is_not_a_proof(retained):
    fixture, verifier, (claim, plan, *_rest) = retained
    changed = replace(claim, limits=replace(claim.limits, max_catalog_rows=claim.limits.max_catalog_rows + 1))
    with pytest.raises(ValueError, match="limits"):
        reader(verifier, fixture).require_plan_membership(fixture.refs, registration=changed, plan_set=plan)


def test_pure_noncharacter_comparison_only(pure_source):
    compare(pure_source)


@pytest.mark.parametrize(
    "path,value",
    [
        (("config", "materialized"), "table"),
        (("config", "enabled"), 1),
        (("config", "contract", "enforced"), False),
        (("config", "meta", "dpone", "publish", "model_storage"), "rowstore_page"),
        (("database",), "other"),
        (("schema",), "other"),
        (("alias",), "other"),
        (("unique_id",), "model.other.id"),
        (("resource_type",), "seed"),
        (("columns", "id", "name"), "renamed"),
        (("columns", "id", "data_type"), "INT"),
        (("columns", "id", "data_type"), "int; DROP TABLE x"),
        (("columns", "id", "constraints"), []),
        (("columns", "id", "constraints"), [{"type": "unique"}]),
    ],
)
def test_pure_model_semantic_mismatch(pure_source, path, value):
    data = list(deepcopy(pure_source))
    model_id = data[1].models[0].spec.model_unique_id
    node = data[2]["nodes"][model_id]
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


def test_pure_missing_selected_model_rejects(pure_source):
    data = list(deepcopy(pure_source))
    del data[2]["nodes"][data[1].models[0].spec.model_unique_id]
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


def test_pure_column_order_is_not_sorted(pure_source):
    data = list(deepcopy(pure_source))
    columns = (MssqlCatalogColumn("z", "int", False), MssqlCatalogColumn("a", "int", True))
    data[1] = with_columns(data[1], columns)
    for model in data[1].models:
        data[2]["nodes"][model.spec.model_unique_id]["columns"] = {
            "z": {"name": "z", "data_type": "int", "constraints": [{"type": "not_null"}]},
            "a": {"name": "a", "data_type": "int", "constraints": []},
        }
    compare(data)
    node = data[2]["nodes"][data[1].models[0].spec.model_unique_id]
    node["columns"] = dict(reversed(list(node["columns"].items())))
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


def test_pure_character_collation_is_explicitly_unavailable(pure_source):
    data = list(deepcopy(pure_source))
    data[1] = with_columns(data[1], (MssqlCatalogColumn("id", "nvarchar(5)", False, "Latin1_General_100_BIN2"),))
    for model in data[1].models:
        raw = data[2]["nodes"][model.spec.model_unique_id]["columns"]["id"]
        raw.update(
            data_type="nvarchar(5)", collation="Latin1_General_100_BIN2", meta={"collation": "Latin1_General_100_BIN2"}
        )
    with pytest.raises(PhysicalPlanMembershipError, match="^DPONE_PHYSICAL_PLAN_COLLATION_UNAVAILABLE$") as error:
        compare(data)
    assert "native_execution.physical_collation.name" in error.value.remediation


@pytest.mark.parametrize("field", ["source_graph_sha256", "filegroup", "resource_bounds"])
def test_pure_plan_projection_mismatch(pure_source, field):
    from dpone.contracts.dbt_mssql_physical import PhysicalFilegroup
    from dpone.contracts.native_identity import OriginalRef

    data = list(deepcopy(pure_source))
    replacement = {
        "source_graph_sha256": "sha256:" + "b" * 64,
        "filegroup": PhysicalFilegroup(1, "other"),
        "resource_bounds": OriginalRef("other", "sha256:" + "c" * 64),
    }[field]
    data[1] = replace(
        data[1],
        models=tuple(replace(model, spec=replace(model.spec, **{field: replacement})) for model in data[1].models),
    )
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


def test_pure_extra_plan_model_rejects_full_membership(pure_source):
    data = list(deepcopy(pure_source))
    model = data[1].models[0]
    extra = replace(model, spec=replace(model.spec, model_unique_id="model.extra.unselected"))
    data[1] = replace(
        data[1], models=tuple(sorted((*data[1].models, extra), key=lambda item: item.spec.model_unique_id))
    )
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


def test_pure_upstream_model_cannot_be_omitted(pure_source):
    from dpone.contracts.dbt_selection_lock import DbtSelectionLock

    data = list(deepcopy(pure_source))
    lock = data[3]
    fields = lock.to_dict()
    fields.pop("schema")
    fields.pop("selection_sha256")
    for name in ("selectors", "expected_run_result_unique_ids", "publish_model_unique_ids"):
        fields[name] = tuple(fields[name])
    fields["selected_graph_unique_ids"] = tuple(sorted((*lock.selected_graph_unique_ids, "model.upstream.hidden")))
    data[3] = DbtSelectionLock.build(**fields)
    data[2]["nodes"]["model.upstream.hidden"] = {"unique_id": "model.upstream.hidden", "resource_type": "model"}
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


def test_pure_retained_resolved_target_differs_from_base(pure_source):
    assert pure_source[4]["authoring_template"]["invocation_target"]["schema"] == "base"
    assert {model.spec.relation.schema for model in pure_source[1].models} == {"alpha"}
    compare(pure_source)


def test_adapter_preserves_resolved_target_after_base_check(retained, monkeypatch):
    import dpone.adapters.dbt_mssql_physical_plan_membership as module

    fixture, verifier, (claim, plan, *_rest) = retained
    actual = module.require_model_plan_membership
    observed = []

    def record(*args, **kwargs):
        observed.append(kwargs["logical_target"])
        return actual(*args, **kwargs)

    # Observe arguments only: the real comparator still rejects the legacy source.
    monkeypatch.setattr(module, "require_model_plan_membership", record)
    with pytest.raises(PhysicalPlanMembershipError, match="MANAGED_SOURCE_REQUIRED"):
        reader(verifier, fixture).require_plan_membership(fixture.refs, registration=claim, plan_set=plan)
    assert observed == [("analytics", "alpha")]


def test_retained_schema_errors_are_rejected(tmp_path, monkeypatch):
    fixture = retained_fixture(tmp_path, monkeypatch, malformed_schema=True)
    with _native_verifier(fixture, []) as verifier:
        claim, plan, *_rest = inputs(verifier, fixture)
        with pytest.raises(PhysicalPlanMembershipError, match="MANIFEST_INVALID"):
            reader(verifier, fixture).require_plan_membership(fixture.refs, registration=claim, plan_set=plan)


@pytest.mark.parametrize("dtype", ["char(5)", "varchar(5)", "nchar(5)", "nvarchar(5)"])
def test_pure_character_expectation_comes_from_selected_policy(pure_source, dtype):
    data = list(deepcopy(pure_source))
    name = "Latin1_General_100_BIN2"
    data[4]["native_execution"]["physical_collation"] = {"name": name}
    data[1] = with_columns(data[1], (MssqlCatalogColumn("id", dtype, False, name),))
    for model in data[1].models:
        data[2]["nodes"][model.spec.model_unique_id]["columns"]["id"]["data_type"] = dtype
    compare(data)
    data[4]["native_execution"]["physical_collation"]["name"] = name.lower()
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


def test_pure_selected_collation_does_not_add_noncharacter_requirement(pure_source):
    data = list(deepcopy(pure_source))
    data[4]["native_execution"]["physical_collation"] = {"name": "Latin1_General_100_BIN2"}
    compare(data)


def test_pure_mixed_character_collations_reject(pure_source):
    data = list(deepcopy(pure_source))
    name = "Latin1_General_100_BIN2"
    data[4]["native_execution"]["physical_collation"] = {"name": name}
    columns = (
        MssqlCatalogColumn("id", "varchar(5)", False, name),
        MssqlCatalogColumn("other", "nvarchar(5)", True, name.lower()),
    )
    data[1] = with_columns(data[1], columns)
    for model in data[1].models:
        data[2]["nodes"][model.spec.model_unique_id]["columns"] = {
            column.name: {
                "name": column.name,
                "data_type": column.dtype,
                "constraints": [] if column.nullable else [{"type": "not_null"}],
            }
            for column in columns
        }
    with pytest.raises(PhysicalPlanMembershipError):
        compare(data)


@pytest.mark.parametrize("name", ["Latin1_General_100_BIN2", "SQL_Latin1_General_CP1_CI_AS"])
def test_real_policy_collation_substitution_rejects_authenticated_inventory(retained, name):
    import json

    from dpone.adapters.native_project_documents import NativeProjectDocumentReader
    from dpone.contracts.native_project_documents import NATIVE_POLICY_MEMBER

    fixture, verifier, (claim, plan, *_rest) = retained
    changed_members = []

    def read(root, path, *, max_bytes):
        payload = read_confined_file(root, path, max_bytes=max_bytes)
        if path == NATIVE_POLICY_MEMBER:
            policy = json.loads(payload)
            policy["profiles"]["local"]["native_execution"]["physical_collation"] = {"name": name}
            changed_members.append(path)
            return json.dumps(policy).encode()
        return payload

    actual = MssqlPhysicalPlanMembershipReader(
        verifier=verifier,
        documents=NativeProjectDocumentReader(read_file=read, max_policy_bytes=1024 * 1024),
        read_file=read_confined_file,
        release_root=fixture.release_root,
        max_release_bytes=8 * 1024 * 1024,
    )
    with pytest.raises(ValueError, match="verified archive inventory"):
        actual.require_plan_membership(fixture.refs, registration=claim, plan_set=plan)
    assert changed_members == [NATIVE_POLICY_MEMBER]
    assert all(not resolver.calls for resolver in fixture.resolver_factory.resolvers)
