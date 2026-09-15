"""Native starter policy retention must be lossless and opt-in."""

import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts.dbt_authoring_template import DbtAuthoringTemplate
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry
from tests.test_dbt_authoring_template import payload as template_payload
from tests.test_dbt_native_policy_v4 import native_policy


def test_native_registry_preserves_complete_document_and_selected_member() -> None:
    policy = native_policy()
    policy["profiles"]["local"]["authoring_template"] = template_payload()
    expected = deepcopy(policy)
    registry, issues = DbtPublishProfileRegistry.from_mapping(policy, source_path="explicit.yml")
    assert issues == ()
    assert registry is not None
    profile = registry.profile("local")
    assert profile is not None
    assert registry.native_policy_document == encode_native_delivery_json(expected)
    assert profile.native_profile_payload == encode_native_delivery_json(expected["profiles"]["local"])
    assert profile.native_policy_schema == expected["schema"]
    assert profile.authoring_template == DbtAuthoringTemplate.from_mapping(template_payload())
    policy["profiles"]["local"]["native_execution"]["control"]["schema"] = "changed"
    policy["workflows"]["orders"]["owner"] = "changed"
    assert decode_native_delivery_json(registry.native_policy_document) == expected
    assert decode_native_delivery_json(profile.native_profile_payload) == expected["profiles"]["local"]
    with pytest.raises(AttributeError):
        registry.native_policy_document = b"{}"


def test_native_budget_is_not_the_legacy_source_byte_budget() -> None:
    registry, issues = DbtPublishProfileRegistry.from_mapping(native_policy(), source_path="explicit.yml")
    assert not issues and registry is not None
    strategy = registry.strategy_policy("local")
    assert strategy is not None
    assert strategy.full_refresh_serialized_payload_max_bytes == 16777216
    assert strategy.full_refresh_max_source_bytes is None
    assert strategy.publication_completion_timeout_seconds == 600
    assert strategy.full_refresh_authorized


def test_manual_native_profile_does_not_require_authoring_template() -> None:
    registry, issues = DbtPublishProfileRegistry.from_mapping(native_policy(), source_path="explicit.yml")
    assert not issues and registry is not None
    assert registry.profile("local").authoring_template is None


@pytest.mark.parametrize("mode", ["capacity", "boolean", "legacy_budget", "invalid_template", "unknown_native"])
def test_native_registry_rejects_inconsistent_policies_without_input_content(mode: str) -> None:
    policy = native_policy()
    profile = policy["profiles"]["local"]
    if mode == "capacity":
        profile["native_execution"]["generation"]["max_generation_bytes"] = 1
    elif mode == "boolean":
        profile["native_execution"]["limits"]["max_metadata_bytes"] = True
    elif mode == "legacy_budget":
        profile["strategy_policy"]["full_refresh"]["max_source_bytes"] = 42
    elif mode == "invalid_template":
        profile["authoring_template"] = template_payload()
        profile["authoring_template"]["project_name"] = "PRIVATE_SENTINEL invalid"
    else:
        profile["native_execution"]["unexpected"] = "PRIVATE_SENTINEL"
    registry, issues = DbtPublishProfileRegistry.from_mapping(policy, source_path="explicit.yml")
    assert registry is None and issues
    assert all(issue.code == "DPONE_DBT_PROFILES_INVALID" for issue in issues)
    assert all("PRIVATE_SENTINEL" not in issue.message for issue in issues)


@pytest.mark.parametrize("kind", ["dpone.dbt-publish-policy.v2", "dpone.dbt-publish-policy.v3"])
def test_earlier_policy_versions_retain_their_old_budget_semantics(kind: str) -> None:
    policy = native_policy()
    policy["schema"] = kind
    profile = policy["profiles"]["local"]
    del profile["dbt_model_physical_design"], profile["native_execution"]
    strategy = profile["strategy_policy"]
    del strategy["publication_completion_timeout_seconds"]
    strategy["full_refresh"] = {"authorized": True, "max_source_bytes": 42}
    if kind.endswith("v2"):
        from tests.test_dbt_semantic_refresh_policy_v2 import _v2_policy

        sample = _v2_policy()
        profile["refresh"] = next(iter(sample["profiles"].values()))["refresh"]
    registry, issues = DbtPublishProfileRegistry.from_mapping(policy, source_path="legacy.yml")
    assert not issues and registry is not None
    parsed = registry.profile("local")
    assert parsed.authoring_template is None
    assert parsed.native_policy_schema is None
    assert parsed.native_profile_payload is None
    assert registry.native_policy_document is None
    parsed_strategy = registry.strategy_policy("local")
    assert parsed_strategy.full_refresh_max_source_bytes == 42
    assert parsed_strategy.full_refresh_serialized_payload_max_bytes is None
    assert parsed_strategy.publication_completion_timeout_seconds is None


@pytest.mark.parametrize("change", ["selected_bytes", "workflow", "strategy", "noncanonical", "source"])
def test_direct_registry_construction_cannot_bind_inconsistent_native_views(change: str) -> None:
    registry, issues = DbtPublishProfileRegistry.from_mapping(native_policy(), source_path="explicit.yml")
    assert not issues and registry is not None
    profile = registry.profile("local")
    workflow = registry.workflow("orders")
    strategy = registry.strategy_policy("local")
    document = registry.native_policy_document
    if change == "selected_bytes":
        profile = replace(profile, native_profile_payload=b"{}")
    elif change == "workflow":
        workflow = replace(workflow, owner="changed")
    elif change == "strategy":
        strategy = replace(strategy, full_refresh_serialized_payload_max_bytes=1)
    elif change == "source":
        profile = replace(profile, source_connection_ref="changed")
    else:
        document += b"\n"
    with pytest.raises(ValueError):
        DbtPublishProfileRegistry(
            {"local": profile},
            {"orders": workflow},
            source_path="explicit.yml",
            strategy_policies={"local": strategy},
            native_policy_document=document,
        )


def test_v1_does_not_silently_accept_the_newer_toolchain() -> None:
    policy = native_policy()
    policy["schema"] = "dpone.dbt-publish-policy.v1"
    registry, issues = DbtPublishProfileRegistry.from_mapping(policy, source_path="legacy.yml")
    assert registry is None
    assert {issue.code for issue in issues} == {"DPONE_DBT_PROFILES_INVALID"}


def starter_service():
    from dpone.adapters.project_authoring_lock import project_authoring_lock
    from dpone.readiness.airflow_scaffold_apply import ScaffoldFile
    from dpone.readiness.dbt_starter import DbtStarterService
    from tests.test_dbt_starter_resources import PACKAGE_FILES

    class Resources:
        def files(self):
            templates = {
                "dbt_project.yml": "name: @@DPONE_PROJECT_NAME@@\nprofile: @@DPONE_DBT_PROFILE@@\n",
                "profiles/profiles.yml": "@@DPONE_DBT_PROFILE@@: [@@DPONE_DBT_TARGET@@, @@DPONE_INVOCATION_DATABASE@@, @@DPONE_INVOCATION_SCHEMA@@]\n",
                "models/orders.sql": "{{ config(profile=@@DPONE_PROFILE@@, workflow=@@DPONE_WORKFLOW@@) }}\n",
                "models/schema.yml": "source: [@@DPONE_SOURCE_DATABASE@@, @@DPONE_SOURCE_SCHEMA@@, @@DPONE_SOURCE_NAME@@]\n",
                "README.md": "Synthetic test only, not a runnable or qualified starter.\n",
                ".gitignore": "target/\nlogs/\n",
                "packages.yml": "synthetic dependency fixture\n",
                "package-lock.yml": "synthetic lock fixture\n",
            }
            templates.update(
                {"dbt_packages/dbt_dpone/" + name: "synthetic package fixture\r\n" for name in PACKAGE_FILES}
            )
            return tuple(ScaffoldFile(Path(name), text) for name, text in templates.items())

    return DbtStarterService(
        resources=Resources(),
        parse_profiles=DbtPublishProfileRegistry.from_mapping,
        authoring_lock=project_authoring_lock,
    )


def starter_policy(tmp_path: Path) -> Path:
    policy = native_policy()
    policy["profiles"]["local"]["authoring_template"] = template_payload()
    target = tmp_path / "explicit-policy.yml"
    target.write_bytes(("# exact source snapshot\r\n" + json.dumps(policy, indent=2)).encode())
    return target


def test_starter_dry_run_creates_no_destination_and_suppresses_content(tmp_path: Path) -> None:
    service = starter_service()
    policy = starter_policy(tmp_path)
    target = tmp_path / "new-project"
    result = service.init(target, profiles=policy, profile="local", workflow="orders", dry_run=True)
    assert result.passed
    assert len(result.changes) == 23
    assert all(change.action == "create" and change.diff == "" for change in result.changes)
    assert not target.exists()


def test_starter_apply_noop_and_conflict_preserve_user_files(tmp_path: Path) -> None:
    service = starter_service()
    policy = starter_policy(tmp_path)
    target = tmp_path / "project"
    first = service.init(target, profiles=policy, profile="local", workflow="orders")
    assert first.passed
    assert (target / "dpone/dbt-publish-profiles.yml").read_bytes() == policy.read_bytes()
    second = service.init(target, profiles=policy, profile="local", workflow="orders")
    assert second.passed and all(change.action == "no_op" for change in second.changes)
    model = target / "models/orders.sql"
    model.write_text("PRIVATE_SQL_SENTINEL")
    conflict = service.init(target, profiles=policy, profile="local", workflow="orders")
    assert not conflict.passed
    assert model.read_text() == "PRIVATE_SQL_SENTINEL"
    assert all(change.diff == "" for change in conflict.changes)
    assert "PRIVATE_SQL_SENTINEL" not in str(conflict.to_dict())


@pytest.mark.parametrize("mode", ["missing_profile", "missing_workflow", "missing_template", "invalid_yaml"])
def test_starter_invalid_selection_has_no_destination(tmp_path: Path, mode: str) -> None:
    policy = starter_policy(tmp_path)
    if mode == "missing_template":
        policy.write_text(json.dumps(native_policy()))
    elif mode == "invalid_yaml":
        policy.write_text("duplicate: 1\nduplicate: PRIVATE_SENTINEL\n")
    result = starter_service().init(
        tmp_path / "project",
        profiles=policy,
        profile="absent" if mode == "missing_profile" else "local",
        workflow="absent" if mode == "missing_workflow" else "orders",
    )
    assert not result.passed and result.exit_code == 1
    assert not (tmp_path / "project").exists()
    assert "PRIVATE_SENTINEL" not in str(result.to_dict())


@pytest.mark.parametrize("rollback_fails", [False, True])
def test_starter_partial_apply_rolls_back_and_preserves_existing_file(
    tmp_path: Path, monkeypatch, rollback_fails: bool
) -> None:
    from dpone.readiness.airflow_scaffold_apply import ScaffoldFileSystem

    target = tmp_path / "project"
    target.mkdir()
    existing = target / "user-notes.txt"
    existing.write_text("keep")
    original = ScaffoldFileSystem.create
    count = 0

    def fail_third(self, file):
        nonlocal count
        count += 1
        if count == 3:
            raise OSError("PRIVATE_SQL_SENTINEL")
        return original(self, file)

    monkeypatch.setattr(ScaffoldFileSystem, "create", fail_third)
    if rollback_fails:

        def fail_rollback(self, item):
            raise OSError("PRIVATE_SQL_SENTINEL")

        monkeypatch.setattr(ScaffoldFileSystem, "rollback", fail_rollback)
    result = starter_service().init(target, profiles=starter_policy(tmp_path), profile="local", workflow="orders")
    assert not result.passed and result.exit_code == 4
    assert existing.read_text() == "keep"
    if not rollback_fails:
        assert {path for path in target.rglob("*") if path.is_file()} == {existing}
    assert "PRIVATE_SQL_SENTINEL" not in str(result.to_dict())
    assert result.details["recovery_required"] is rollback_fails


@pytest.mark.parametrize("dry_run", [False, True])
def test_starter_rejects_target_symlink_without_modifying_destination(tmp_path: Path, dry_run: bool) -> None:
    target = tmp_path / "actual"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    result = starter_service().init(
        alias, profiles=starter_policy(tmp_path), profile="local", workflow="orders", dry_run=dry_run
    )
    assert not result.passed
    assert list(target.iterdir()) == []


@pytest.mark.parametrize("format_name", ["text", "json", "md"])
def test_starter_resource_failure_occurs_before_lock_or_destination(tmp_path: Path, capsys, format_name: str) -> None:
    from dpone.commands.init_cmd import _emit_self_service_result
    from dpone.readiness.dbt_starter import DbtStarterService

    class MissingResources:
        def files(self):
            raise ValueError("PRIVATE_SENTINEL")

    def forbidden_lock(path):
        raise AssertionError("lock must not be acquired")

    service = DbtStarterService(
        resources=MissingResources(),
        parse_profiles=DbtPublishProfileRegistry.from_mapping,
        authoring_lock=forbidden_lock,
    )
    result = service.init(tmp_path / "project", profiles=starter_policy(tmp_path), profile="local", workflow="orders")
    assert not result.passed and result.exit_code == 2
    assert result.errors[0]["code"] == "DPONE_DBT_STARTER_RESOURCES_INVALID"
    assert result.errors[0]["docs_url"] == "docs/errors/DPONE_DBT_STARTER_RESOURCES_INVALID.md"
    assert result.errors[0]["fixes"] == [{"id": "repair_installed_distribution", "safety": "manual"}]
    assert "PRIVATE_SENTINEL" not in str(result.to_dict())
    assert not (tmp_path / "project").exists()
    _emit_self_service_result("dpone init dbt", result.to_dict(), format_name)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "DPONE_DBT_STARTER_RESOURCES_INVALID" in captured.out
    assert "repair_installed_distribution" in captured.out
    assert "errors/DPONE_DBT_STARTER_RESOURCES_INVALID" in captured.out
    assert "PRIVATE_SENTINEL" not in captured.out


def test_starter_does_not_consult_environment_for_policy(tmp_path: Path, monkeypatch) -> None:
    from dpone.manifest import dbt_publish_profiles

    class ForbiddenEnvironment:
        def get(self, *args):
            raise AssertionError("unexpected environment policy discovery")

    # Patch only the module's os binding; tempfile/lock may consult OS settings.
    from types import SimpleNamespace

    monkeypatch.setattr(dbt_publish_profiles, "os", SimpleNamespace(environ=ForbiddenEnvironment()))
    result = starter_service().init(
        tmp_path / "project", profiles=starter_policy(tmp_path), profile="local", workflow="orders"
    )
    assert result.passed


@pytest.mark.parametrize("expression", ["{{private}}", "{%private%}", "{#private#}", "@@DPONE_PROFILE@@"])
def test_starter_rejects_scalar_template_expressions(tmp_path: Path, expression: str) -> None:
    policy = native_policy()
    policy["profiles"]["local"]["authoring_template"] = template_payload()
    policy["profiles"]["local"]["runtime"]["dbt_profile"] = expression
    explicit = tmp_path / "policy.yml"
    explicit.write_text(json.dumps(policy))
    result = starter_service().init(tmp_path / "project", profiles=explicit, profile="local", workflow="orders")
    assert not result.passed and result.exit_code == 1
    assert expression not in str(result.to_dict())
    assert not (tmp_path / "project").exists()


def test_starter_uses_one_policy_snapshot_even_if_path_changes_after_parse(tmp_path: Path) -> None:
    from dpone.adapters.project_authoring_lock import project_authoring_lock
    from dpone.readiness.dbt_starter import DbtStarterService

    policy = starter_policy(tmp_path)
    original = policy.read_bytes()
    base = starter_service()

    def parse_and_replace(payload, *, source_path):
        result = DbtPublishProfileRegistry.from_mapping(payload, source_path=source_path)
        policy.write_text("changed after snapshot")
        return result

    service = DbtStarterService(
        resources=base._resources, parse_profiles=parse_and_replace, authoring_lock=project_authoring_lock
    )
    target = tmp_path / "project"
    result = service.init(target, profiles=policy, profile="local", workflow="orders")
    assert result.passed
    assert (target / "dpone/dbt-publish-profiles.yml").read_bytes() == original


def test_starter_performs_no_subprocess_or_network_operations(tmp_path: Path, monkeypatch) -> None:
    import socket
    import subprocess

    def forbidden(*args, **kwargs):
        raise AssertionError("offline init attempted external execution")

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    result = starter_service().init(
        tmp_path / "project", profiles=starter_policy(tmp_path), profile="local", workflow="orders"
    )
    assert result.passed


def test_authored_templates_render_the_managed_analyst_project(tmp_path: Path) -> None:
    import yaml
    from jinja2 import Environment, StrictUndefined

    from dpone.adapters.project_authoring_lock import project_authoring_lock
    from dpone.readiness.airflow_scaffold_apply import ScaffoldFile
    from dpone.readiness.dbt_starter import DbtStarterService
    from tests.test_dbt_starter_resources import STARTER_OUTPUTS

    template_root = Path(__file__).parents[1] / "src/dpone/_assets/dbt_starter/v4"
    base = starter_service()

    class ActualTemplates:
        def files(self):
            synthetic = {file.path.as_posix(): file for file in base._resources.files()}
            for resource, output in STARTER_OUTPUTS.items():
                if resource.endswith(".tmpl"):
                    synthetic[output] = ScaffoldFile(Path(output), (template_root / resource).read_bytes().decode())
            return tuple(synthetic.values())

    service = DbtStarterService(
        resources=ActualTemplates(),
        parse_profiles=DbtPublishProfileRegistry.from_mapping,
        authoring_lock=project_authoring_lock,
    )
    target = tmp_path / "project"
    result = service.init(target, profiles=starter_policy(tmp_path), profile="local", workflow="orders")
    assert result.passed
    project = yaml.safe_load((target / "dbt_project.yml").read_text())
    assert project["name"] == "orders_demo" and project["profile"] == "native"
    assert set(project["clean-targets"]) == {"target", "logs"}
    assert "models" not in project and "dispatch" not in project
    assert project["flags"] == {
        "dbt_sqlserver_enable_safe_type_expansion": False,
        "dbt_sqlserver_use_native_string_types": True,
        "dbt_sqlserver_use_dbt_transactions": True,
        "dbt_sqlserver_use_default_schema_concat": True,
    }
    profiles = yaml.safe_load((target / "profiles/profiles.yml").read_text())
    connection = profiles["native"]["outputs"]["local"]
    assert connection["database"] == "Development" and connection["schema"] == "authoring"
    assert connection["server"] == "localhost" and connection["threads"] == 1
    assert connection["encrypt"] is True and connection["trust_cert"] is False
    assert "env_var" not in (target / "profiles/profiles.yml").read_text()
    schema = yaml.safe_load((target / "models/schema.yml").read_text())
    source = schema["sources"][0]
    assert (source["database"], source["schema"], source["tables"][0]["identifier"]) == ("Source", "dbo", "orders")
    columns = schema["models"][0]["columns"]
    assert columns[0]["constraints"] == [{"type": "not_null"}]
    assert columns[0]["data_tests"] == ["not_null"]
    assert [(column["name"], column["data_type"]) for column in columns] == [
        ("order_id", "bigint"),
        ("order_total", "decimal(18,2)"),
    ]
    captured = {}

    def capture_config(**values):
        captured.update(values)
        return ""

    sql = (
        Environment(undefined=StrictUndefined)
        .from_string((target / "models/orders.sql").read_text())
        .render(config=capture_config, source=lambda source, table: "resolved_source")
    )
    assert "from resolved_source" in sql
    assert captured["materialized"] == "dpone_managed_table"
    assert captured["contract"] == {"enforced": True}
    assert captured["meta"]["dpone"]["publish"] == {
        "schema": "dpone.dbt-publish-authoring.v2",
        "enabled": True,
        "profile": "local",
        "workflow": "orders",
        "strategy": {"mode": "full_refresh"},
        "model_storage": "rowstore_none",
    }


@pytest.mark.parametrize("format_name", ["text", "json", "md"])
def test_invalid_policy_keys_and_values_do_not_enter_diagnostics(tmp_path: Path, capsys, format_name: str) -> None:
    from dpone.commands.init_cmd import _emit_self_service_result

    policy = native_policy()
    policy["profiles"]["local"]["PRIVATE_KEY_SENTINEL"] = {"password": "PRIVATE_VALUE_SENTINEL"}
    path = tmp_path / "policy.yml"
    path.write_text(json.dumps(policy))
    result = starter_service().init(tmp_path / "project", profiles=path, profile="local", workflow="orders")
    assert not result.passed
    assert all(error["path"] == str(path) for error in result.errors)
    _emit_self_service_result("dpone init dbt", result.to_dict(), format_name)
    rendered = capsys.readouterr()
    assert "PRIVATE_KEY_SENTINEL" not in rendered.out
    assert "PRIVATE_VALUE_SENTINEL" not in rendered.out
    assert rendered.err == ""
    assert not (tmp_path / "project").exists()
