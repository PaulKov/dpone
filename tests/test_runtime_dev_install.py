from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.dev_install import (
    DEFAULT_PACKAGE_TARGET,
    RuntimeInstallConfig,
    RuntimeInstallError,
    build_runtime_exec_env,
    build_runtime_install_command,
    build_runtime_install_plan,
    compute_runtime_pythonpath,
    read_runtime_install_config,
    run_runtime_exec,
)


def test_read_runtime_install_config_defaults() -> None:
    cfg = read_runtime_install_config({})
    assert cfg.install_mode == "baked"
    assert cfg.package_spec is None
    assert cfg.target_dir == DEFAULT_PACKAGE_TARGET
    assert cfg.pip_args == ()


def test_read_runtime_install_config_snapshot() -> None:
    cfg = read_runtime_install_config(
        {
            "DPONE_INSTALL_MODE": "snapshot",
            "DPONE_PACKAGE_SPEC": "dpone==1.2.3.dev4",
            "DPONE_PACKAGE_INDEX_URL": "https://registry/simple",
            "DPONE_PACKAGE_EXTRA_INDEX_URL": "https://extra/simple",
            "DPONE_PACKAGE_EXTRA_INDEX_URLS": "https://extra-2/simple https://extra-3/simple",
            "DPONE_PACKAGE_TARGET": "/tmp/site",
            "DPONE_PACKAGE_PIP_ARGS": "--trusted-host gitlab.example.com",
        }
    )
    assert cfg.install_mode == "snapshot"
    assert cfg.package_spec == "dpone==1.2.3.dev4"
    assert cfg.index_url == "https://registry/simple"
    assert cfg.extra_index_url == "https://extra/simple"
    assert cfg.extra_index_urls == ("https://extra-2/simple", "https://extra-3/simple")
    assert cfg.all_extra_index_urls == (
        "https://extra/simple",
        "https://extra-2/simple",
        "https://extra-3/simple",
    )
    assert cfg.target_dir == "/tmp/site"
    assert cfg.pip_args == ("--trusted-host", "gitlab.example.com")


def test_snapshot_requires_package_spec() -> None:
    with pytest.raises(RuntimeInstallError):
        read_runtime_install_config({"DPONE_INSTALL_MODE": "snapshot"})


def test_build_runtime_install_command_contains_target_and_indexes() -> None:
    cfg = RuntimeInstallConfig(
        install_mode="snapshot",
        package_spec="dpone==1.2.3.dev4",
        index_url="https://registry/simple",
        extra_index_url="https://extra/simple",
        extra_index_urls=("https://extra-2/simple", "https://extra/simple"),
        target_dir="/tmp/site",
        pip_args=("--trusted-host", "gitlab.example.com"),
    )
    cmd = build_runtime_install_command(cfg, python_executable="python")
    assert cmd[:6] == ["python", "-m", "pip", "install", "--upgrade", "--target"]
    assert "/tmp/site" in cmd
    assert "https://registry/simple" in cmd
    assert cmd.count("--extra-index-url") == 2
    assert "https://extra/simple" in cmd
    assert "https://extra-2/simple" in cmd
    assert cmd[-1] == "dpone==1.2.3.dev4"


def test_compute_runtime_pythonpath_prepends_target() -> None:
    cfg = RuntimeInstallConfig(install_mode="snapshot", package_spec="dpone==1", target_dir="/tmp/site")
    result = compute_runtime_pythonpath(cfg, current_pythonpath="/a:/b")
    assert result.split(":")[0].endswith("/tmp/site")
    assert "/a" in result
    assert "/b" in result


def test_build_runtime_exec_env_updates_pythonpath() -> None:
    cfg = RuntimeInstallConfig(install_mode="snapshot", package_spec="dpone==1", target_dir="/tmp/site")
    env = build_runtime_exec_env(cfg, env={"PYTHONPATH": "/x"})
    assert env["PYTHONPATH"].split(":")[0].endswith("/tmp/site")
    assert "/x" in env["PYTHONPATH"]


def test_build_runtime_install_plan_includes_exec_command() -> None:
    cfg = RuntimeInstallConfig(install_mode="snapshot", package_spec="dpone==1", target_dir="/tmp/site")
    plan = build_runtime_install_plan(cfg, python_executable="python", command=["airflow", "scheduler"])
    assert plan["needs_install"] is True
    assert plan["install_command"]
    assert plan["exec_command"] == ["airflow", "scheduler"]


def test_build_runtime_install_plan_includes_all_extra_indexes() -> None:
    cfg = RuntimeInstallConfig(
        install_mode="snapshot",
        package_spec="dpone==1",
        extra_index_url="https://extra/simple",
        extra_index_urls=("https://extra-2/simple",),
        target_dir="/tmp/site",
    )
    plan = build_runtime_install_plan(cfg, python_executable="python")
    assert plan["extra_index_url"] == "https://extra/simple"
    assert plan["extra_index_urls"] == ["https://extra/simple", "https://extra-2/simple"]


def test_run_runtime_exec_installs_and_execs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: dict[str, object] = {}

    def fake_run(cmd, check):
        seen["install_cmd"] = list(cmd)
        seen["check"] = check
        return 0

    def fake_exec(file, argv, env):
        seen["exec_file"] = file
        seen["exec_argv"] = list(argv)
        seen["exec_env"] = dict(env)
        raise SystemExit(0)

    cfg = RuntimeInstallConfig(
        install_mode="snapshot",
        package_spec="dpone==1.2.3.dev4",
        index_url="https://registry/simple",
        target_dir=str(tmp_path / "overlay"),
    )

    with pytest.raises(SystemExit):
        run_runtime_exec(
            ["python", "-m", "dpone.cli.main", "--help"],
            cfg,
            python_executable="python",
            env={"PYTHONPATH": "/base"},
            runner=fake_run,
            execer=fake_exec,
        )

    assert seen["install_cmd"][-1] == "dpone==1.2.3.dev4"
    assert seen["exec_file"] == "python"
    assert seen["exec_argv"][:2] == ["python", "-m"]
    assert seen["exec_env"]["PYTHONPATH"].split(":")[0].endswith("overlay")


def test_pyproject_exposes_runtime_bootstrap_scripts() -> None:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib

    data = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = data["project"]["scripts"]
    assert scripts["dpone-runtime-install"] == "dpone.runtime.dev_install:main_install"
    assert scripts["dpone-runtime-exec"] == "dpone.runtime.dev_install:main_exec"
