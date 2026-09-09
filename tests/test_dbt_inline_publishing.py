from __future__ import annotations

import json
import os
import subprocess
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.adapters.dbt_sqlserver_graph_policy import (
    DbtSqlserverPreviewGraphPolicyValidator,
)
from dpone.adapters.dbt_workflow_selection import (
    ManifestPreviewSelectionResolver,
)
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_publish_composition import (
    DbtPublishProfileLoader,
)
from dpone.app.dbt_publish_composition import (
    build_dbt_dpone_compiler as _build_dbt_dpone_compiler,
)
from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.dag.config_models import ETLProcessConfig
from dpone.manifest.dbt_publish_intent_resolver import DbtPublishIntentResolver
from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry
from dpone.readiness.dbt_publish_capability_policy import DbtRouteCapabilityPolicy
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)
from dpone.services.dbt_publish_artifact_writer import DbtArtifactWriter
from dpone.services.dbt_publish_compiler import DbtDponeCompiler
from dpone.services.dbt_publish_model_compiler import DbtModelToWorkloadCompiler
from dpone.services.dbt_publish_planning import DbtPublishPlanner

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples" / "dbt-inline-publishing"
MANIFEST = DEMO / "fixtures" / "manifest.v12.json"
PROFILES = DEMO / "dpone" / "dbt-publish-profiles.yml"
PROFILE_NAME = "mssql_to_clickhouse_mart"


def build_dbt_dpone_compiler(
    *,
    root: Path | None = None,
    require_certified_routes: bool = False,
) -> DbtDponeCompiler:
    """Bind synthetic manifest variants to the checked-in demo project."""

    return _build_dbt_dpone_compiler(
        root=DEMO if root is None else root,
        require_certified_routes=require_certified_routes,
    )


def _preview_artifact_writer() -> DbtArtifactWriter:
    return DbtArtifactWriter(
        selection_resolver=ManifestPreviewSelectionResolver(),
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
    )


def _strategy_policy(
    payload: dict,
    *,
    allowed: list[str] | None = None,
) -> dict:
    policy = {
        "allowed_strategies": allowed if allowed is not None else ["incremental_merge", "partition_replace"],
        "partition_replace": {"require_atomic_capability": True},
    }
    payload["profiles"][PROFILE_NAME]["strategy_policy"] = policy
    return policy


def test_reader_supports_manifest_v10_to_v12(tmp_path: Path) -> None:
    class AcceptOfficialSchema:
        def validate(self, payload, *, version):
            del payload, version
            return ()

    source = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for version in (10, 11, 12):
        source["metadata"]["dbt_schema_version"] = f"https://schemas.getdbt.com/dbt/manifest/v{version}.json"
        path = tmp_path / f"manifest-v{version}.json"
        path.write_text(json.dumps(source), encoding="utf-8")
        artifact, issues = DbtArtifactReader(validator=AcceptOfficialSchema()).read(path)
        assert issues == ()
        assert artifact is not None
        assert artifact.schema_version == version
        assert len(artifact.models) == 2


def test_demo_compiles_explicit_and_auto_strategy() -> None:
    report = build_dbt_dpone_compiler().build(MANIFEST, profiles_path=PROFILES)
    assert report.passed
    assert len(report.workflows) == 1
    strategies = {item.model.name: item.strategy for item in report.models}
    assert strategies["competitive_pricing"]["mode"] == "partition_replace"
    assert strategies["competitive_pricing_history"]["mode"] == "incremental_merge"
    assert {item.workload_id for item in report.models} == {
        "dbt_competitive_pricing",
        "dbt_competitive_pricing_history",
    }
    assert strategies["competitive_pricing_history"]["decision_reason"] == "dbt_incremental_with_unique_key"
    manifests = {item.model.name: item.manifest for item in report.models}
    assert manifests["competitive_pricing"]["state"] == {
        "type": "mssql",
        "connection_ref": "mssql_dwh_stage",
        "table": {"schema": "dpone_state", "name": "dpone_xmin_state"},
        "partition_checkpoint_table": {
            "schema": "dpone_state",
            "name": "dpone_partition_checkpoints",
        },
    }
    predicate = manifests["competitive_pricing"]["source"]["options"]["source_custom_predicate"]
    assert "{{ data_interval_start }}" in predicate
    assert "{{ data_interval_end }}" in predicate
    assert "{ data_interval_start }" not in predicate.replace("{{ data_interval_start }}", "")


def test_demo_real_parse_validates_and_can_publish_the_generated_manifest(
    tmp_path: Path,
) -> None:
    generated_manifest = tmp_path / "manifest.json"
    completed = subprocess.run(
        ("bash", "examples/dbt-inline-publishing/run_demo.sh"),
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
        env={
            **os.environ,
            "DPONE_DBT_DEMO_MODE": "parse",
            "DPONE_DBT_DEMO_MANIFEST_OUTPUT": str(generated_manifest),
        },
    )

    assert completed.returncode == 0, completed.stderr
    assert f"Generated manifest: {generated_manifest}" in completed.stdout
    assert "Manifest source: real dbt parse in a temporary project copy" in completed.stdout
    assert "Demo PASS: validated 2 publish-enabled models in 1 workflow." in completed.stdout
    payload = json.loads(generated_manifest.read_text(encoding="utf-8"))
    history = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing_history"]
    for key in ("product_id", "date_id"):
        constraints = history["columns"][key]["constraints"]
        assert [constraint["type"] for constraint in constraints] == ["not_null"]


def test_graph_policy_scopes_logical_targets_per_workflow(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    history_id = "model.dpone_dbt_demo.competitive_pricing_history"
    current_id = "model.dpone_dbt_demo.competitive_pricing"
    history = payload["nodes"][history_id]
    history["database"] = "DWH_History"
    history["schema"] = "archive"
    history["config"]["database"] = "DWH_History"
    history["config"]["schema"] = "archive"
    history["config"]["meta"]["dpone"]["publish"]["workflow"] = "pricing_history"
    history["depends_on"]["nodes"] = []
    payload["parent_map"][history_id] = []
    payload["child_map"][current_id] = [child for child in payload["child_map"][current_id] if child != history_id]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    profiles = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    profiles["workflows"]["pricing_history"] = {
        **profiles["workflows"]["competitive_pricing"],
        "owner": "pricing-history-data",
    }
    profiles_path = tmp_path / "dbt-publish-profiles.yml"
    profiles_path.write_text(yaml.safe_dump(profiles), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(
        manifest,
        profiles_path=profiles_path,
    )

    assert report.passed
    assert {workflow.workflow for workflow in report.workflows} == {
        "competitive_pricing",
        "pricing_history",
    }


def test_cross_workflow_publish_dependency_fails_before_compile(
    tmp_path: Path,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    history_id = "model.dpone_dbt_demo.competitive_pricing_history"
    history = payload["nodes"][history_id]
    history["config"]["meta"]["dpone"]["publish"]["workflow"] = "pricing_history"
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    profiles = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    profiles["workflows"]["pricing_history"] = {
        **profiles["workflows"]["competitive_pricing"],
        "owner": "pricing-history-data",
    }
    profiles_path = tmp_path / "dbt-publish-profiles.yml"
    profiles_path.write_text(yaml.safe_dump(profiles), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(
        manifest,
        profiles_path=profiles_path,
    )

    assert not report.passed
    assert [issue.code for issue in report.blockers] == [
        "DPONE_DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED",
    ]
    assert "competitive_pricing" in report.blockers[0].message
    assert "pricing_history" in report.blockers[0].message


def test_eager_relationship_test_cannot_read_a_foreign_workflow_model(
    tmp_path: Path,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    current_id = "model.dpone_dbt_demo.competitive_pricing"
    history_id = "model.dpone_dbt_demo.competitive_pricing_history"
    relationship_id = "test.dpone_dbt_demo.pricing_relationship"
    history = payload["nodes"][history_id]
    history["config"]["meta"]["dpone"]["publish"]["workflow"] = "pricing_history"
    history["depends_on"]["nodes"] = []
    payload["parent_map"][history_id] = []
    payload["child_map"][current_id] = [child for child in payload["child_map"][current_id] if child != history_id]
    template = next(node for node in payload["nodes"].values() if node.get("resource_type") == "test")
    relationship = deepcopy(template)
    relationship.update(
        {
            "unique_id": relationship_id,
            "name": "pricing_relationship",
            "attached_node": None,
            "depends_on": {
                "macros": ["macro.dbt.test_relationships"],
                "nodes": [current_id, history_id],
            },
        }
    )
    payload["nodes"][relationship_id] = relationship
    payload["parent_map"][relationship_id] = [current_id, history_id]
    payload["child_map"][current_id].append(relationship_id)
    payload["child_map"][history_id].append(relationship_id)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    profiles = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    profiles["workflows"]["pricing_history"] = {
        **profiles["workflows"]["competitive_pricing"],
        "owner": "pricing-history-data",
    }
    profiles_path = tmp_path / "dbt-publish-profiles.yml"
    profiles_path.write_text(yaml.safe_dump(profiles), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(
        manifest,
        profiles_path=profiles_path,
    )

    assert not report.passed
    assert {issue.code for issue in report.blockers} == {
        "DPONE_DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW",
    }
    assert all(issue.path.endswith("schema.yml") for issue in report.blockers)
    assert all(issue.remediation for issue in report.blockers)


def test_shared_non_publish_model_fails_project_level_preview(
    tmp_path: Path,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    current_id = "model.dpone_dbt_demo.competitive_pricing"
    history_id = "model.dpone_dbt_demo.competitive_pricing_history"
    shared_id = "model.dpone_dbt_demo.shared_pricing_stage"
    current = payload["nodes"][current_id]
    history = payload["nodes"][history_id]
    history["config"]["meta"]["dpone"]["publish"]["workflow"] = "pricing_history"

    shared = deepcopy(current)
    shared.update(
        {
            "unique_id": shared_id,
            "name": "shared_pricing_stage",
            "fqn": ["dpone_dbt_demo", "shared_pricing_stage"],
            "alias": "shared_pricing_stage",
            "relation_name": "[DWH].[pricing].[shared_pricing_stage]",
            "original_file_path": "models/shared_pricing_stage.sql",
        }
    )
    shared["config"]["meta"] = {}
    shared["depends_on"]["nodes"] = []
    payload["nodes"][shared_id] = shared
    current["depends_on"]["nodes"] = [shared_id]
    history["depends_on"]["nodes"] = [shared_id]
    payload["parent_map"][shared_id] = []
    payload["parent_map"][current_id] = [shared_id]
    payload["parent_map"][history_id] = [shared_id]
    payload["child_map"][shared_id] = [current_id, history_id]
    payload["child_map"][current_id] = [child for child in payload["child_map"][current_id] if child != history_id]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    profiles = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    profiles["workflows"]["pricing_history"] = {
        **profiles["workflows"]["competitive_pricing"],
        "owner": "pricing-history-data",
    }
    profiles_path = tmp_path / "dbt-publish-profiles.yml"
    profiles_path.write_text(yaml.safe_dump(profiles), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(
        manifest,
        profiles_path=profiles_path,
    )

    assert not report.passed
    assert [issue.code for issue in report.blockers] == [
        "DPONE_DBT_WORKFLOW_GRAPH_OVERLAP",
    ]
    assert shared_id in report.blockers[0].message
    assert report.blockers[0].path.endswith("shared_pricing_stage.sql")
    assert "disjoint" in report.blockers[0].remediation


@pytest.mark.parametrize(
    ("constraint_key", "constraints"),
    [
        (
            "column",
            [{"type": "unique"}],
        ),
        (
            "model",
            [{"type": "primary_key", "columns": ["product_id"]}],
        ),
    ],
)
def test_unsupported_physical_constraints_fail_during_check(
    tmp_path: Path,
    constraint_key: str,
    constraints: list[dict[str, object]],
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    model = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    if constraint_key == "column":
        model["columns"]["product_id"]["constraints"] = constraints
    else:
        model["constraints"] = constraints
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(
        manifest,
        profiles_path=PROFILES,
    )

    assert not report.passed
    assert {issue.code for issue in report.blockers} == {
        "DPONE_DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED",
    }


def test_stateful_strategy_requires_platform_owned_state_policy(
    tmp_path: Path,
) -> None:
    profile_payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    del profile_payload["profiles"][PROFILE_NAME]["state"]
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(profile_payload, sort_keys=False), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(
        MANIFEST,
        profiles_path=profile_path,
    )

    assert not report.passed
    assert "DPONE_DBT_STATE_POLICY_REQUIRED" in {issue.code for issue in report.blockers}


def test_explicit_strategy_is_blocked_when_profile_does_not_allow_it(
    tmp_path: Path,
) -> None:
    profile_payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    _strategy_policy(profile_payload, allowed=["incremental_merge"])
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(
        yaml.safe_dump(profile_payload, sort_keys=False),
        encoding="utf-8",
    )

    report = build_dbt_dpone_compiler().build(
        MANIFEST,
        profiles_path=profile_path,
    )

    assert not report.passed
    assert "DPONE_DBT_STRATEGY_UNRESOLVED" in {issue.code for issue in report.blockers}


def test_authorized_full_refresh_freezes_platform_byte_budget(
    tmp_path: Path,
) -> None:
    manifest_payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for node in manifest_payload["nodes"].values():
        if node.get("resource_type") == "model":
            node["config"]["meta"]["dpone"]["publish"]["strategy"] = {"mode": "full_refresh"}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")
    profile_payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    strategy_policy = _strategy_policy(
        profile_payload,
        allowed=["full_refresh"],
    )
    strategy_policy["full_refresh"] = {
        "authorized": True,
        "max_source_bytes": 1_000_000_000,
    }
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(
        yaml.safe_dump(profile_payload, sort_keys=False),
        encoding="utf-8",
    )

    report = build_dbt_dpone_compiler().build(
        manifest_path,
        profiles_path=profile_path,
    )

    assert report.passed
    assert {model.strategy["max_source_bytes"] for model in report.models} == {1_000_000_000}


def test_compiler_resolves_replicated_clickhouse_design() -> None:
    report = build_dbt_dpone_compiler().build(MANIFEST, profiles_path=PROFILES)
    design = report.models[0].physical_design["storage"]["clickhouse"]
    assert design["engine"].startswith("ReplicatedMergeTree")
    assert design["cluster"] == "dwh"


def test_secret_like_meta_is_blocked(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    node["config"]["meta"]["dpone"]["publish"]["password"] = "must-not-enter-artifacts"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)
    assert not report.passed
    assert "DPONE_DBT_SECRET_FORBIDDEN" in {item.code for item in report.blockers}
    assert "must-not-enter-artifacts" not in json.dumps(report.to_jsonable())


def test_missing_contract_is_production_blocker(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    node["config"]["contract"]["enforced"] = False
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)
    assert "DPONE_DBT_CONTRACT_REQUIRED" in {item.code for item in report.blockers}


def test_conflicting_workflow_parallelism_is_blocked(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing_history"]
    node["config"]["meta"]["dpone"]["publish"]["execution"]["max_parallelism"] = 3
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)
    assert "DPONE_DBT_WORKFLOW_PARALLELISM_CONFLICT" in {item.code for item in report.blockers}


def test_zero_enabled_models_fails_closed_unless_explicitly_allowed(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for node in payload["nodes"].values():
        if node.get("resource_type") == "model":
            node["config"]["meta"]["dpone"]["publish"]["enabled"] = False
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    compiler = build_dbt_dpone_compiler()

    blocked = compiler.build(path)

    assert not blocked.passed
    assert {item.code for item in blocked.blockers} == {"DPONE_DBT_NO_PUBLISH_MODELS"}

    allowed = compiler.build(path, allow_empty=True)
    output = tmp_path / "must-not-exist"
    written = _preview_artifact_writer().write(allowed, output)

    assert allowed.passed
    assert allowed.models == ()
    assert allowed.workflows == ()
    assert written.artifacts == {}
    assert not output.exists()


def test_unknown_model_selector_fails_without_compiling_other_models() -> None:
    report = build_dbt_dpone_compiler().build(
        MANIFEST,
        profiles_path=PROFILES,
        model_selector="does_not_exist",
    )

    assert not report.passed
    assert report.models == ()
    assert report.workflows == ()
    assert {item.code for item in report.blockers} == {"DPONE_DBT_MODEL_NOT_FOUND"}


def test_ambiguous_short_model_selector_requires_exact_identity(
    tmp_path: Path,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for node in payload["nodes"].values():
        if node.get("resource_type") == "model":
            node["alias"] = "duplicate_alias"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(
        path,
        profiles_path=PROFILES,
        model_selector="duplicate_alias",
    )

    assert not report.passed
    assert report.models == ()
    assert {item.code for item in report.blockers} == {"DPONE_DBT_MODEL_AMBIGUOUS"}


@pytest.mark.parametrize(
    "workflow",
    ("../prod", "sales/daily", "SalesDaily", "-daily", "a" * 65),
)
def test_unsafe_workflow_id_fails_before_artifact_writes(
    tmp_path: Path,
    workflow: str,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for node in payload["nodes"].values():
        if node.get("resource_type") == "model":
            node["config"]["meta"]["dpone"]["publish"]["workflow"] = workflow
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)
    output = tmp_path / "must-not-exist"
    with pytest.raises(
        ValueError,
        match="Cannot write artifacts for a blocked dbt publish compile",
    ):
        _preview_artifact_writer().write(
            report,
            output,
            project_root=DEMO,
        )

    assert not report.passed
    assert {item.code for item in report.blockers} == {"DPONE_DBT_WORKFLOW_ID_INVALID"}
    assert not output.exists()


def test_auto_strategy_uses_next_capability_supported_candidate(
    tmp_path: Path,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    history = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing_history"]
    history["config"]["meta"]["dpone"]["publish"]["strategy"] = {
        "mode": "auto",
        "partition_key": "date_id",
        "window_days": 45,
    }
    current = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    current["config"]["meta"]["dpone"]["publish"]["enabled"] = False
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    class PartitionOnlyCapabilities:
        def resolve(self, **request):
            if request["strategy"] == "partition_replace":
                return {
                    "support": "supported",
                    "route_certification": "production-certified",
                }, ()
            return None, (
                DbtPublishIssue(
                    code="DPONE_DBT_ROUTE_NOT_SUPPORTED",
                    message="merge is unavailable",
                    path=request["path"],
                ),
            )

    compiler = DbtDponeCompiler(
        reader=DbtArtifactReader(),
        resolver=DbtPublishIntentResolver(),
        model_compiler=DbtModelToWorkloadCompiler(
            planner=DbtPublishPlanner(),
        ),
        profile_loader=DbtPublishProfileLoader(),
        route_capabilities=PartitionOnlyCapabilities(),
        project_policy=DbtSqlserverProjectPolicyValidator(project_root=DEMO),
        graph_policy=DbtSqlserverPreviewGraphPolicyValidator(),
    )
    report = compiler.build(path, profiles_path=PROFILES)

    assert report.passed
    assert len(report.models) == 1
    assert report.models[0].strategy["mode"] == "partition_replace"
    assert report.models[0].strategy["decision_reason"] == ("certified_partition_window")


@pytest.mark.parametrize("value", ["true", 1, 0, None])
def test_publish_enabled_requires_a_json_boolean(tmp_path: Path, value: object) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    node["config"]["meta"]["dpone"]["publish"]["enabled"] = value
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)

    assert not report.passed
    assert "DPONE_DBT_INTENT_INVALID" in {item.code for item in report.blockers}


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("lineage", "enabled", "false"),
        ("lineage", "enabled", 1),
        ("strategy", "window_days", "45"),
        ("strategy", "window_days", 1.5),
        ("strategy", "window_days", True),
        ("execution", "max_parallelism", "2"),
        ("execution", "max_parallelism", 2.0),
        ("execution", "max_parallelism", False),
    ],
)
def test_publish_scalars_do_not_coerce_json_values(
    tmp_path: Path,
    section: str,
    field: str,
    value: object,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    node["config"]["meta"]["dpone"]["publish"][section][field] = value
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)

    assert not report.passed
    assert "DPONE_DBT_INTENT_INVALID" in {item.code for item in report.blockers}


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("publish", "unexpected", True),
        ("target", "database", "DWH"),
        ("strategy", "window", 45),
        ("physical_design", "engine", "MergeTree()"),
        ("physical_design", "partition_by", "toYYYYMM(date_id)"),
        ("execution", "queue", "default"),
        ("quality", "threshold", 0.99),
        ("lineage", "backend", "custom"),
    ],
)
def test_publish_authoring_is_closed_and_rejects_raw_physical_overrides(
    tmp_path: Path,
    section: str,
    field: str,
    value: object,
) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    publish = node["config"]["meta"]["dpone"]["publish"]
    if section == "publish":
        publish[field] = value
    else:
        publish.setdefault(section, {})[field] = value
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)

    assert not report.passed
    expected_code = (
        "DPONE_DBT_RAW_PHYSICAL_OVERRIDE_FORBIDDEN" if section == "physical_design" else "DPONE_DBT_INTENT_INVALID"
    )
    assert expected_code in {item.code for item in report.blockers}


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("workflows", "competitive_pricing", "catchup"), "false"),
        (("workflows", "competitive_pricing", "max_active_runs"), "1"),
        (("profiles", "mssql_to_clickhouse_mart", "runtime", "dbt_threads"), "4"),
        (("profiles", "mssql_to_clickhouse_mart", "execution", "deferrable"), "true"),
        (("profiles", "mssql_to_clickhouse_mart", "execution", "max_parallelism"), 2.5),
        (
            (
                "profiles",
                "mssql_to_clickhouse_mart",
                "source",
                "options",
                "native_transfer",
                "mode",
            ),
            1,
        ),
        (
            (
                "profiles",
                "mssql_to_clickhouse_mart",
                "sink",
                "options",
                "load_governance",
                "audit",
                "enabled",
            ),
            "true",
        ),
        (
            (
                "profiles",
                "mssql_to_clickhouse_mart",
                "strategy_policy",
                "allowed_strategies",
            ),
            "incremental_merge",
        ),
        (("workflows", "competitive_pricing", "tags"), "pricing"),
    ],
)
def test_profile_registry_does_not_coerce_scalars(
    tmp_path: Path,
    path: tuple[str, ...],
    value: object,
) -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    if "strategy_policy" in path:
        _strategy_policy(payload)
    cursor = payload
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = value
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, profile_path)

    assert registry is None
    assert {item.code for item in issues} == {"DPONE_DBT_PROFILES_INVALID"}


@pytest.mark.parametrize(
    "path",
    [
        ("unexpected",),
        ("profiles", "mssql_to_clickhouse_mart", "unexpected"),
        ("profiles", "mssql_to_clickhouse_mart", "source", "unexpected"),
        (
            "profiles",
            "mssql_to_clickhouse_mart",
            "source",
            "options",
            "native_transfer",
            "unexpected",
        ),
        (
            "profiles",
            "mssql_to_clickhouse_mart",
            "sink",
            "options",
            "load_governance",
            "audit",
            "unexpected",
        ),
        ("profiles", "mssql_to_clickhouse_mart", "runtime", "unexpected"),
        ("profiles", "mssql_to_clickhouse_mart", "execution", "unexpected"),
        ("profiles", "mssql_to_clickhouse_mart", "quality", "unexpected"),
        ("profiles", "mssql_to_clickhouse_mart", "lineage", "unexpected"),
        (
            "profiles",
            "mssql_to_clickhouse_mart",
            "strategy_policy",
            "unexpected",
        ),
        ("workflows", "competitive_pricing", "unexpected"),
    ],
)
def test_profile_registry_rejects_unknown_fields(
    tmp_path: Path,
    path: tuple[str, ...],
) -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    if "strategy_policy" in path:
        _strategy_policy(payload)
    cursor = payload
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = "unexpected"
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, profile_path)

    assert registry is None
    assert {item.code for item in issues} == {"DPONE_DBT_PROFILES_INVALID"}


def test_profile_registry_accepts_strict_allowed_strategies(tmp_path: Path) -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    _strategy_policy(payload)
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, profile_path)

    assert registry is not None
    assert issues == ()
    policy = registry.strategy_policy(PROFILE_NAME)
    assert policy is not None
    assert policy.allowed_strategies == ("incremental_merge", "partition_replace")
    assert policy.partition_replace_requires_atomic_capability is True
    assert not policy.allows("full_refresh")


def test_profile_registry_defaults_to_non_destructive_strategy_policy() -> None:
    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, PROFILES)

    assert registry is not None
    assert issues == ()
    policy = registry.strategy_policy(PROFILE_NAME)
    assert policy is not None
    assert policy.allowed_strategies == ("incremental_merge", "partition_replace")
    assert policy.full_refresh_authorized is False
    assert policy.full_refresh_max_source_bytes is None
    assert policy.partition_replace_requires_atomic_capability is True


def test_profile_registry_reads_exact_certification_dimensions() -> None:
    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, PROFILES)

    assert registry is not None
    assert issues == ()
    profile = registry.profile(PROFILE_NAME)
    assert profile is not None
    assert profile.certification is not None
    assert profile.certification.transport == "native_bcp_to_clickhouse"
    assert profile.certification.schema_evolution == "widening"
    assert profile.certification.airflow_runtime_mode == "kpo"


def test_route_capability_selects_one_exact_certification_variant() -> None:
    evidence_ref = "sha256:" + "a" * 64
    selected = SimpleNamespace(
        id="mssql_clickhouse_incremental_merge_airflow_kpo",
        route_id="mssql:clickhouse:incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="kpo",
        level="production-certified",
        evidence_status="PASS",
        evidence_refs=(evidence_ref,),
        reason_codes=(),
    )
    other = SimpleNamespace(
        **{
            **vars(selected),
            "id": "mssql_clickhouse_incremental_merge_internal",
            "airflow_runtime_mode": "internal",
        }
    )
    route = SimpleNamespace(
        id="mssql:clickhouse:incremental_merge",
        support=SimpleNamespace(status="supported"),
        certification=SimpleNamespace(variants=(selected, other)),
    )
    snapshot = SimpleNamespace(
        snapshot_id="sha256:" + "b" * 64,
        routes=(route,),
    )

    capability, issues = DbtRouteCapabilityPolicy(
        snapshot,
        require_certified=True,
    ).resolve(
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="kpo",
        path="models/mart.sql",
    )

    assert issues == ()
    assert capability == {
        "snapshot_id": "sha256:" + "b" * 64,
        "route_id": "mssql:clickhouse:incremental_merge",
        "support": "supported",
        "variant_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
        "transport": "native_bcp_to_clickhouse",
        "schema_evolution": "widening",
        "airflow_runtime_mode": "kpo",
        "certification_level": "production-certified",
        "evidence_status": "PASS",
        "evidence_refs": [evidence_ref],
        "evidence_reason_codes": [],
    }


def test_route_capability_rejects_certification_for_other_runtime_variant() -> None:
    variant = SimpleNamespace(
        id="mssql_clickhouse_incremental_merge_internal",
        route_id="mssql:clickhouse:incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="internal",
        level="production-certified",
        evidence_status="PASS",
        evidence_refs=("sha256:" + "a" * 64,),
        reason_codes=(),
    )
    snapshot = SimpleNamespace(
        snapshot_id="sha256:" + "b" * 64,
        routes=(
            SimpleNamespace(
                id="mssql:clickhouse:incremental_merge",
                support=SimpleNamespace(status="supported"),
                certification=SimpleNamespace(variants=(variant,)),
            ),
        ),
    )

    capability, issues = DbtRouteCapabilityPolicy(
        snapshot,
        require_certified=True,
    ).resolve(
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="kpo",
        path="models/mart.sql",
    )

    assert capability is None
    assert {issue.code for issue in issues} == {"DPONE_DBT_ROUTE_NOT_CERTIFIED"}


def test_route_capability_rejects_duplicate_exact_variants() -> None:
    variant = SimpleNamespace(
        id="mssql_clickhouse_incremental_merge_airflow_kpo",
        route_id="mssql:clickhouse:incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="kpo",
        level="production-certified",
        evidence_status="PASS",
        evidence_refs=("sha256:" + "a" * 64,),
        reason_codes=(),
    )
    duplicate = SimpleNamespace(**{**vars(variant), "id": f"{variant.id}_duplicate"})
    snapshot = SimpleNamespace(
        snapshot_id="sha256:" + "b" * 64,
        routes=(
            SimpleNamespace(
                id=variant.route_id,
                support=SimpleNamespace(status="supported"),
                certification=SimpleNamespace(variants=(variant, duplicate)),
            ),
        ),
    )

    capability, issues = DbtRouteCapabilityPolicy(
        snapshot,
        require_certified=True,
    ).resolve(
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        transport=variant.transport,
        schema_evolution=variant.schema_evolution,
        airflow_runtime_mode=variant.airflow_runtime_mode,
        path="models/mart.sql",
    )

    assert capability is None
    assert {issue.code for issue in issues} == {"DPONE_DBT_ROUTE_CERTIFICATION_AMBIGUOUS"}


def test_preview_can_read_deprecated_physical_overrides_with_warning() -> None:
    artifact, read_issues = DbtArtifactReader().read(MANIFEST)
    assert artifact is not None
    assert not any(issue.severity == "error" for issue in read_issues)
    model = next(item for item in artifact.models if item.name == "competitive_pricing")
    meta = deepcopy(model.meta)
    physical = meta["dpone"]["publish"]["physical_design"]
    physical["engine"] = "MergeTree()"
    physical["partition_by"] = "toYYYYMM(date_id)"

    intent, issues = DbtPublishIntentResolver(allow_legacy_physical_overrides=True).resolve(replace(model, meta=meta))

    assert intent is not None
    assert intent.engine == "MergeTree()"
    assert intent.partition_by == "toYYYYMM(date_id)"
    assert {item.code for item in issues} == {"DPONE_DBT_RAW_PHYSICAL_OVERRIDE_DEPRECATED"}
    assert all(item.severity == "warning" for item in issues)


def test_certified_compile_rejects_raw_physical_overrides(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    model = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    model["config"]["meta"]["dpone"]["publish"]["physical_design"] = {
        "engine": "MergeTree()",
        "partition_by": "toYYYYMM(date_id)",
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    (tmp_path / "dbt_project.yml").write_bytes((DEMO / "dbt_project.yml").read_bytes())

    report = build_dbt_dpone_compiler(
        root=tmp_path,
        require_certified_routes=True,
    ).build(
        manifest_path,
        profiles_path=PROFILES,
    )

    assert not report.passed
    assert "DPONE_DBT_RAW_PHYSICAL_OVERRIDE_FORBIDDEN" in {item.code for item in report.blockers}


@pytest.mark.parametrize("strategies", [[], ["auto"], ["incremental_merge", "incremental_merge"]])
def test_profile_registry_rejects_invalid_allowed_strategies(
    tmp_path: Path,
    strategies: list[str],
) -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    _strategy_policy(payload, allowed=strategies)
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, profile_path)

    assert registry is None
    assert {item.code for item in issues} == {"DPONE_DBT_PROFILES_INVALID"}


@pytest.mark.parametrize(
    ("allowed", "full_refresh"),
    [
        (["full_refresh"], None),
        (["full_refresh"], {"authorized": False, "max_source_bytes": 1_000_000}),
        (["full_refresh"], {"authorized": True}),
        (
            ["incremental_merge"],
            {"authorized": True, "max_source_bytes": 1_000_000},
        ),
        (
            ["full_refresh"],
            {"authorized": True, "max_source_bytes": "1000000"},
        ),
    ],
)
def test_profile_registry_rejects_unsafe_full_refresh_policy(
    tmp_path: Path,
    allowed: list[str],
    full_refresh: dict | None,
) -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    policy = _strategy_policy(payload, allowed=allowed)
    if full_refresh is not None:
        policy["full_refresh"] = full_refresh
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, profile_path)

    assert registry is None
    assert {item.code for item in issues} == {"DPONE_DBT_PROFILES_INVALID"}


def test_profile_registry_accepts_bounded_full_refresh_grant(tmp_path: Path) -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    policy_payload = _strategy_policy(payload, allowed=["full_refresh"])
    policy_payload["full_refresh"] = {
        "authorized": True,
        "max_source_bytes": 1_000_000_000,
    }
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, profile_path)

    assert registry is not None
    assert issues == ()
    policy = registry.strategy_policy(PROFILE_NAME)
    assert policy is not None
    assert policy.allows("full_refresh")
    assert policy.full_refresh_max_source_bytes == 1_000_000_000


@pytest.mark.parametrize("value", [False, "true", 1])
def test_profile_registry_requires_atomic_partition_capability(
    tmp_path: Path,
    value: object,
) -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    policy = _strategy_policy(payload, allowed=["partition_replace"])
    policy["partition_replace"]["require_atomic_capability"] = value
    profile_path = tmp_path / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    registry, issues = DbtPublishProfileRegistry.load(MANIFEST, profile_path)

    assert registry is None
    assert {item.code for item in issues} == {"DPONE_DBT_PROFILES_INVALID"}


def test_dbt_contract_enforced_requires_a_json_boolean(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    node["config"]["contract"]["enforced"] = "false"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)

    assert not report.passed
    assert "DPONE_DBT_MANIFEST_INVALID" in {item.code for item in report.blockers}


def test_workload_id_does_not_duplicate_existing_workflow_prefix(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    node["alias"] = "competitive_pricing_current"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)

    assert report.passed
    current = next(item for item in report.models if item.model.name == "competitive_pricing")
    assert current.workload_id == "dbt_competitive_pricing_current"


def test_writer_emits_runtime_manifests_packs_dag_and_evidence(tmp_path: Path) -> None:
    report = build_dbt_dpone_compiler().build(MANIFEST, profiles_path=PROFILES)
    output = tmp_path / "airflow"
    written = _preview_artifact_writer().write(report, output, project_root=DEMO)
    assert written.passed
    assert {key.split(":", 1)[0] for key in written.artifacts} == {
        "manifest",
        "pack",
        "dag",
        "runtime",
        "release",
        "evidence",
    }
    dag_path = output / written.artifacts["dag:DAG__pricing__competitive_pricing__refresh"]
    dag = json.loads(dag_path.read_text(encoding="utf-8"))
    assert dag["wiring"]["max_parallel_workloads"] == 2
    assert dag["wiring"]["mode"] == "explicit"
    assert len(dag["nodes"]) == 3
    assert {edge["upstream"] for edge in dag["edges"]} == {"dbt__competitive_pricing"}
    dbt_pack_path = output / written.artifacts["pack:dbt__competitive_pricing"]
    dbt_pack = json.loads(dbt_pack_path.read_text(encoding="utf-8"))
    assert dbt_pack["runtime_command"] == ""
    assert dbt_pack["provider_execution"]["schema"] == "dpone.airflow-provider-execution.v1"
    assert dbt_pack["runtime_payload_ids"] == [
        "dbt_project",
        "dbt_manifest",
        "dbt_selection_competitive_pricing",
    ]
    assert dbt_pack["kpo_kwargs"]["cmds"] == ["dpone"]
    assert dbt_pack["kpo_kwargs"]["execution_timeout_seconds"] == 3900
    assert dbt_pack["kpo_kwargs"]["arguments"] == [
        "dbt",
        "execute-pack",
        "runtime/dbt-execution-pack.json",
        "--format",
        "json",
    ]
    serialized_pack = json.dumps(dbt_pack)
    assert all(
        token not in serialized_pack for token in ("/bin/sh", "exit 0", "/airflow/xcom/return.json", "status=$?")
    )
    assert dbt_pack["xcom"]["sidecar_image"].endswith("@sha256:" + "b" * 64)
    assert "outcome_gate" not in dbt_pack
    assert dag["default_args"]["retries"] == 0
    release = json.loads((output / "release-set.json").read_text(encoding="utf-8"))
    assert release["schema"] == "dpone.release-set.v2"
    assert release["release_id"].startswith("sha256:")
    assert len(release["artifacts"]["runtime_payloads"]) == 3
    manifest_path = output / written.artifacts["manifest:dbt_competitive_pricing"]
    process = ETLProcessConfig.from_yaml_metadata(manifest_path)
    assert process.load_strategy.value == "partition_replace"
    assert process.load_config.target_schema == "DWH_Stage"


def test_writer_keeps_transfers_as_parallel_siblings(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for node in payload["nodes"].values():
        if node.get("resource_type") == "model":
            node["config"]["meta"]["dpone"]["publish"]["execution"]["max_parallelism"] = 1
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)
    output = tmp_path / "airflow"

    written = _preview_artifact_writer().write(report, output, project_root=DEMO)

    dag_path = output / written.artifacts["dag:DAG__pricing__competitive_pricing__refresh"]
    dag = json.loads(dag_path.read_text(encoding="utf-8"))
    assert dag["wiring"]["mode"] == "explicit"
    assert dag["wiring"]["max_parallel_workloads"] == 1
    assert len(dag["edges"]) == len(report.models)
    assert {edge["upstream"] for edge in dag["edges"]} == {"dbt__competitive_pricing"}
    assert {edge["downstream"] for edge in dag["edges"]} == {item.workload_id for item in report.models}


def test_optional_macro_is_dictionary_only() -> None:
    macro = (ROOT / "packages" / "dbt-dpone" / "macros" / "dpone_publish.sql").read_text(encoding="utf-8")
    assert "return({'dpone': {'publish': publish}})" in macro
    assert all(token not in macro.lower() for token in ("run_query", "requests", "socket", "boto", "airflow"))


def test_profile_registry_contains_no_plaintext_credentials() -> None:
    payload = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))
    serialized = json.dumps(payload).lower()
    assert "password" not in serialized
    assert "secret_key" not in serialized


@pytest.mark.parametrize("mode", ["ephemeral", "view"])
def test_unsupported_or_uncontracted_model_fails_closed(tmp_path: Path, mode: str) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    node = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    node["config"]["materialized"] = mode
    if mode == "view":
        node["config"]["contract"]["enforced"] = False
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    report = build_dbt_dpone_compiler().build(path, profiles_path=PROFILES)
    assert not report.passed
    if mode == "ephemeral":
        assert {issue.code for issue in report.blockers} == {"DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED"}
