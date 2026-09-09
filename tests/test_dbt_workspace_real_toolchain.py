"""Real offline dbt parse/selection; route receipts remain synthetic, not LIVE."""

import importlib.metadata
import json
import shutil
import subprocess
import tarfile
from io import BytesIO
from pathlib import Path

import pytest
import yaml

from dpone.adapters.dbt_executable import current_environment_dbt_executable
from dpone.app.dbt_publish_composition import build_dbt_dpone_compiler
from dpone.app.dbt_workspace_composition import _workspace_writer
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport, DbtWorkspaceProjectCheck
from dpone.manifest.dbt_workspace_discovery import DbtWorkspaceDiscovery
from dpone.services.dbt_workspace import DbtWorkspaceService
from tests.test_dbt_airflow_release_e2e import DEMO, _certified


@pytest.fixture
def exact_dbt():
    expected = {"dbt-core": "1.12.3", "dbt-sqlserver": "1.11.1"}
    executable = current_environment_dbt_executable()
    try:
        versions = {key: importlib.metadata.version(key) for key in expected}
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("certified dbt distributions are not installed in the active environment")
    if versions != expected or not Path(executable).is_file():
        pytest.skip("certified dbt executable/version is unavailable")
    return executable


def _profile(name):
    return {
        name: {
            "target": f"parse_{name}",
            "outputs": {
                f"parse_{name}": {
                    "type": "sqlserver",
                    "driver": "ODBC Driver 18 for SQL Server",
                    "server": "localhost",
                    "port": 1433,
                    "database": "DWH_Stage",
                    "schema": name,
                    "authentication": "sql",
                    "user": "parse_only",
                    "password": "NOT_A_CREDENTIAL_PARSE_ONLY",
                    "encrypt": True,
                    "trust_cert": True,
                    "threads": 1,
                }
            },
        }
    }


def _prepare_project(root, name):
    project = root / name
    shutil.copytree(DEMO, project)
    config_path = project / "dbt_project.yml"
    config = config_path.read_text().replace("dpone_dbt_demo", name)
    config_path.write_text(
        config.replace("+schema: pricing", f"+schema: {name}").replace(
            "workflow: competitive_pricing", f"workflow: {name}"
        )
    )
    for model in (project / "models").glob("*.sql"):
        model.write_text(
            model.read_text()
            .replace("'workflow': 'competitive_pricing'", f"'workflow': '{name}'")
            .replace("'schema': 'DWH_Stage'", f"'schema': 'DWH_Stage_{name}'")
        )
    policy = project / "dpone/dbt-publish-profiles.yml"
    policy.write_text(
        policy.read_text()
        .replace("  competitive_pricing:", f"  {name}:")
        .replace("dbt_profile: dpone_runtime", f"dbt_profile: {name}")
        .replace("dbt_target: runtime", f"dbt_target: parse_{name}")
        .replace("target_schema: DWH_Stage", f"target_schema: DWH_Stage_{name}")
    )
    return project


def _parse(executable, project, profile_dir, state):
    state.mkdir()
    invocation = DbtInvocationContext.canonical()
    completed = subprocess.run(
        (
            executable,
            "--quiet",
            "--no-use-colors",
            "parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(profile_dir),
            "--profile",
            project.name,
            "--target",
            f"parse_{project.name}",
            "--target-path",
            str(project / "target"),
            "--log-path",
            str(state / "logs"),
            "--vars",
            invocation.selection_vars_json(),
        ),
        cwd=project,
        env=invocation.environment(home=str(state / "home")),
        check=False,
        shell=False,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=120,
    )
    assert completed.returncode == 0, "real fixture dbt parse failed"


@pytest.mark.parametrize("shared", [False, True])
def test_two_project_compile_uses_real_parse_and_selection(tmp_path, exact_dbt, shared):
    root = tmp_path / "workspace"
    root.mkdir()
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    shared_profiles = {}
    for name in ("alpha", "beta"):
        shared_profiles.update(_profile(name))
    (shared_dir / "profiles.yml").write_text(yaml.safe_dump(shared_profiles))
    for name in ("alpha", "beta"):
        project = _prepare_project(root, name)
        (project / "profiles.yml").write_text(yaml.safe_dump(_profile(name)))
        _parse(exact_dbt, project, shared_dir if shared else project, tmp_path / f"parse-{name}")

    discovery = DbtWorkspaceDiscovery(environment={}).discover(root)
    assert discovery.passed, discovery.blockers
    rows = []
    for project in discovery.projects:
        project_root = root / project.project_path
        report = build_dbt_dpone_compiler(root=project_root).build(
            project_root / project.manifest_path, profiles_path=project_root / project.profiles_path
        )
        assert report.passed, report.blockers
        # This test proves real graph acquisition, NOT connector certification.
        rows.append(DbtWorkspaceProjectCheck(project, _certified(report)))
    check = DbtWorkspaceCheckReport(discovery, tuple(rows))

    class Service(DbtWorkspaceService):
        def check(self, root):
            return check

    service = Service(
        discovery=object(),
        compiler_factory=object(),
        writer_factory=lambda: _workspace_writer(dbt_profiles_dir=shared_dir if shared else None),
    )
    output = tmp_path / "release"
    report = service.compile(root, output_dir=output)
    assert report.passed, report.blockers
    assert len(report.check.projects) == 2
    release = json.loads((output / "release-set.json").read_bytes())
    assert len(release["artifacts"]["runtime_payloads"]) == 6
    inspected_archives = 0
    for descriptor in release["artifacts"]["runtime_payloads"]:
        payload = (output / descriptor["path"]).read_bytes()
        if descriptor["kind"] == "dbt_project_bundle":
            inspected_archives += 1
            with tarfile.open(fileobj=BytesIO(payload), mode="r:gz") as archive:
                assert all(Path(member.name).name != "profiles.yml" for member in archive.getmembers())
                assert all(
                    b"NOT_A_CREDENTIAL_PARSE_ONLY" not in archive.extractfile(member).read()
                    for member in archive.getmembers()
                    if member.isfile()
                )
    assert inspected_archives == 2
    assert "NOT_A_CREDENTIAL_PARSE_ONLY" not in json.dumps(report.to_dict())
    _verify_runtime_targets(output, tmp_path / "runtime", exact_dbt)

    # Mutating only the logical parse target must not pass source comparison.
    selected = shared_dir if shared else root / "beta"
    profiles_path = selected / "profiles.yml"
    profiles = yaml.safe_load(profiles_path.read_text())
    profiles["beta"]["outputs"]["parse_beta"]["schema"] = "wrong_target"
    profiles_path.write_text(yaml.safe_dump(profiles))
    failed = service.compile(root, output_dir=tmp_path / "wrong-target")
    assert not failed.passed and failed.exit_code == 2
    assert failed.release_id is None and len(failed.check.projects) == 2
    assert not (tmp_path / "wrong-target").exists()


def _verify_runtime_targets(release_root, runtime_root, executable):
    """Execute only actual parse/ls from immutable source bundles, never build."""

    from dpone.adapters.dbt_artifacts import LocalDbtRunResultsReader
    from dpone.adapters.dbt_manifest_schema import OfficialDbtManifestValidator
    from dpone.adapters.dbt_runtime_profile import RuntimeDbtProfileRenderer, TemporaryDbtProfileStore
    from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner
    from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
    from dpone.contracts.dbt_contract_validation import DbtPublishingError
    from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_reference
    from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
    from dpone.manifest.confined_files import read_confined_file
    from dpone.runtime.credentials.config import CredentialsConfig
    from dpone.runtime.dbt_execution_policy import prepare_dbt_output_paths
    from dpone.runtime.dbt_preflight import DbtRuntimePreflight
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader

    release = json.loads((release_root / "release-set.json").read_bytes())
    bundles = RuntimeDbtProjectBundleOperations()
    sources = DbtReleaseSourceReader(bundle_operations=bundles, read_file=read_confined_file).read(
        release_root, expected_release_id=release["release_id"]
    )

    class ParseOnlyRunner(SubprocessDbtCommandRunner):
        def run(self, args, **kwargs):
            assert "build" not in args and ("parse" in args or "ls" in args)
            return super().run(args, **kwargs)

    class DummyResolver:
        def __init__(self, schema):
            self.schema = schema

        def resolve(self, connection_ref):
            return ResolvedBindingConnection(
                credentials=CredentialsConfig(
                    host="localhost",
                    port=1433,
                    database="DWH_Stage",
                    schema=self.schema,
                    username="parse_only",
                    password="NOT_A_CREDENTIAL_PARSE_ONLY",
                    encrypt=True,
                    trust_server_certificate=True,
                ),
                safe_metadata={"resolver": "offline_fixture", "resolved_version": "1"},
                descriptor=ResolvedConnectionDescriptor(connection_type="mssql", properties={}),
            )

    for source in sources.workflows:
        pack = source.execution
        name = source.project.project_name
        assert pack.schema == "dpone.dbt-execution-pack.v2"
        assert pack.invocation_target.schema == name
        assert pack.profile.schema == f"{name}_{name}"
        state = runtime_root / name
        state.mkdir(parents=True)
        reference = dbt_runtime_payload_reference(
            source.source.runtime_payload_ids[0], wire_contract=DBT_RUNTIME_WIRE_V2
        )
        project = state / "dbt-project"
        bundles.extract((release_root / reference.path).read_bytes(), project)
        profiles = TemporaryDbtProfileStore(state / "profiles")
        preflight = DbtRuntimePreflight(
            command_runner=ParseOnlyRunner(dbt_executable=executable),
            artifact_reader=LocalDbtRunResultsReader(),
            manifest_validator=OfficialDbtManifestValidator(),
        )
        for incorrect_base in (False, True):
            profile = pack.profile if incorrect_base else pack.invocation_profile()
            rendered = RuntimeDbtProfileRenderer(DummyResolver(profile.schema)).render(profile, pack.adapter_runtime)
            output_root = state / ("wrong-base" if incorrect_base else "correct-base")
            output_root.mkdir()
            output = prepare_dbt_output_paths(output_root, pack.target_path, attempt_id="a" * 32)
            with profiles.materialize(rendered.content) as profile_path:
                arguments = dict(
                    project_dir=project,
                    profile_path=profile_path,
                    output_paths=output,
                    interval_vars_json=pack.invocation_context.selection_vars_json(),
                    redactions=rendered.redaction_values,
                )
                if incorrect_base:
                    with pytest.raises(DbtPublishingError) as caught:
                        preflight.verify(pack, **arguments)
                    assert caught.value.code == "DPONE_DBT_TARGET_IDENTITY_MISMATCH"
                else:
                    observed = preflight.verify(pack, **arguments)
                    assert observed.graph_contract_sha256 == pack.selection_lock.graph_contract_sha256
                    assert observed.expected_run_result_unique_ids == pack.selection_lock.expected_run_result_unique_ids
