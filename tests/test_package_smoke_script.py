from __future__ import annotations

import importlib.util
import shlex
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]


def _load_module() -> ModuleType:
    path = ROOT / "tools" / "package_smoke.py"
    spec = importlib.util.spec_from_file_location("package_smoke", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_smoke_commands_contains_expected_cli_calls(monkeypatch) -> None:
    module = _load_module()
    monkeypatch.setenv("DPONE_CMD", "python -m dpone.cli.main")
    commands = module.build_smoke_commands(
        project_root=ROOT,
        manifest_rel="examples/batch/landing_postgres_to_bq.batch.yaml",
        registry_rel="examples/registry/sources.yaml",
        selector="public.core_city",
    )
    assert commands[0] == ["python", "-m", "dpone.cli.main", "--help"]
    manifest = ROOT / "examples/batch/landing_postgres_to_bq.batch.yaml"
    manifest_path = str(manifest.resolve())
    assert any(
        cmd[-3:] == [manifest_path, "--registry", str((ROOT / "examples/registry/sources.yaml").resolve())]
        and "validate" in cmd
        for cmd in commands
    )
    dag_report = next(cmd for cmd in commands if "dag" in cmd and "report" in cmd)
    assert dag_report[dag_report.index("--base-path") + 1] == str(manifest.resolve().parent)


def test_build_smoke_plan_includes_default_api_cases(monkeypatch) -> None:
    module = _load_module()
    monkeypatch.setenv("DPONE_CMD", "dpone")
    commands = module.build_smoke_plan(project_root=ROOT, cases=module.DEFAULT_SMOKE_CASES, dpone_cmd="dpone")
    rendered = [" ".join(cmd) for cmd in commands]
    assert any("landing_appsflyer_api.batch.yaml" in item for item in rendered)
    assert any("landing_cbr_api.batch.yaml" in item for item in rendered)
    assert any("landing_mindbox_api.batch.yaml" in item for item in rendered)
    assert any("landing_fasttrack_api.batch.yaml" in item for item in rendered)
    assert any("landing_similarweb_api.batch.yaml" in item for item in rendered)
    assert any("landing_openexchangerates_api.batch.yaml" in item for item in rendered)
    assert any("landing_google_ads_api.batch.yaml" in item for item in rendered)
    assert any("landing_google_sheets_api.batch.yaml" in item for item in rendered)
    assert any("landing_yandex_webmaster_api.batch.yaml" in item for item in rendered)


def test_run_smoke_commands_sets_project_dir_and_checks_semantics(monkeypatch) -> None:
    module = _load_module()
    seen = []

    class Result:
        def __init__(self, stdout: str, returncode: int = 0) -> None:
            self.stdout = stdout
            self.stderr = ""
            self.returncode = returncode

    def fake_run(cmd, *, cwd, env, capture_output, text, check):
        seen.append((cmd, cwd, env.get("DPONE_PROJECT_DIR")))
        joined = " ".join(cmd)
        if "manifest validate" in joined:
            return Result("OK: no issues\n")
        if "manifest render" in joined:
            return Result("sink:\nsource:\n")
        if "dag report" in joined:
            return Result('{"summary": {"task_count": 2}}')
        return Result("usage: dpone ...\n")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    commands = module.build_smoke_plan(project_root=ROOT, cases=module.DEFAULT_SMOKE_CASES, dpone_cmd="dpone")
    module.run_smoke_commands(commands, project_root=ROOT)
    assert len(seen) == len(commands)
    assert all(item[1] == str(ROOT) for item in seen)
    assert all(item[2] == str(ROOT) for item in seen)


def test_default_package_smoke_validates_real_example_registry_bindings() -> None:
    """Keep release smoke inputs executable, beyond mocked command assembly."""
    module = _load_module()
    commands = module.build_smoke_plan(
        project_root=ROOT,
        cases=module.DEFAULT_SMOKE_CASES,
        dpone_cmd=f"{shlex.quote(sys.executable)} -m dpone.cli.main",
    )
    module.run_smoke_commands(commands, project_root=ROOT)
