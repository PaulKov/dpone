from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_INSTALL_MODE = "baked"
DEFAULT_PACKAGE_TARGET = "/opt/airflow/.dpone-pkgs"
_ALLOWED_INSTALL_MODES = {"baked", "snapshot"}


@dataclass(frozen=True)
class RuntimeInstallConfig:
    install_mode: str = DEFAULT_INSTALL_MODE
    package_spec: str | None = None
    index_url: str | None = None
    extra_index_url: str | None = None
    extra_index_urls: tuple[str, ...] = ()
    target_dir: str = DEFAULT_PACKAGE_TARGET
    pip_args: tuple[str, ...] = ()

    @property
    def needs_install(self) -> bool:
        return self.install_mode == "snapshot"

    @property
    def all_extra_index_urls(self) -> tuple[str, ...]:
        ordered: list[str] = []
        if self.extra_index_url:
            ordered.append(self.extra_index_url)
        ordered.extend(self.extra_index_urls)
        return tuple(dict.fromkeys(ordered))


class RuntimeInstallError(RuntimeError):
    """Raised when runtime install mode settings are invalid or installation fails."""


def read_runtime_install_config(env: Mapping[str, str] | None = None) -> RuntimeInstallConfig:
    source = env or os.environ
    install_mode = str(source.get("DPONE_INSTALL_MODE", DEFAULT_INSTALL_MODE)).strip().lower() or DEFAULT_INSTALL_MODE
    package_spec = _clean_optional(source.get("DPONE_PACKAGE_SPEC"))
    index_url = _clean_optional(source.get("DPONE_PACKAGE_INDEX_URL") or source.get("PIP_INDEX_URL"))
    extra_index_url = _clean_optional(source.get("DPONE_PACKAGE_EXTRA_INDEX_URL") or source.get("PIP_EXTRA_INDEX_URL"))
    extra_index_urls = tuple(
        cleaned
        for value in shlex.split(source.get("DPONE_PACKAGE_EXTRA_INDEX_URLS", ""))
        if (cleaned := _clean_optional(value)) is not None
    )
    target_dir = str(source.get("DPONE_PACKAGE_TARGET", DEFAULT_PACKAGE_TARGET)).strip() or DEFAULT_PACKAGE_TARGET
    pip_args = tuple(shlex.split(source.get("DPONE_PACKAGE_PIP_ARGS", "")))
    config = RuntimeInstallConfig(
        install_mode=install_mode,
        package_spec=package_spec,
        index_url=index_url,
        extra_index_url=extra_index_url,
        extra_index_urls=extra_index_urls,
        target_dir=target_dir,
        pip_args=pip_args,
    )
    validate_runtime_install_config(config)
    return config


def validate_runtime_install_config(config: RuntimeInstallConfig) -> None:
    if config.install_mode not in _ALLOWED_INSTALL_MODES:
        allowed = ", ".join(sorted(_ALLOWED_INSTALL_MODES))
        raise RuntimeInstallError(f"Unsupported DPONE_INSTALL_MODE={config.install_mode!r}. Allowed values: {allowed}")
    if config.needs_install and not config.package_spec:
        raise RuntimeInstallError("DPONE_PACKAGE_SPEC is required when DPONE_INSTALL_MODE=snapshot")
    if not config.target_dir:
        raise RuntimeInstallError("DPONE_PACKAGE_TARGET must be a non-empty writable directory path")


def build_runtime_install_command(
    config: RuntimeInstallConfig,
    *,
    python_executable: str | None = None,
) -> list[str]:
    validate_runtime_install_config(config)
    if not config.needs_install:
        return []
    python_bin = python_executable or sys.executable
    cmd = [
        python_bin,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--target",
        config.target_dir,
    ]
    if config.index_url:
        cmd.extend(["--index-url", config.index_url])
    for extra_index_url in config.all_extra_index_urls:
        cmd.extend(["--extra-index-url", extra_index_url])
    cmd.extend(config.pip_args)
    cmd.append(str(config.package_spec))
    return cmd


def compute_runtime_pythonpath(config: RuntimeInstallConfig, *, current_pythonpath: str | None = None) -> str:
    current = (current_pythonpath or "").strip()
    if not config.needs_install:
        return current
    target = str(Path(config.target_dir).resolve())
    parts = [part for part in current.split(os.pathsep) if part]
    if target in parts:
        parts = [p for p in parts if p != target]
    return os.pathsep.join([target, *parts]) if parts else target


def build_runtime_exec_env(
    config: RuntimeInstallConfig,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    merged = dict(env or os.environ)
    if config.needs_install:
        merged["PYTHONPATH"] = compute_runtime_pythonpath(config, current_pythonpath=merged.get("PYTHONPATH"))
    return merged


def build_runtime_install_plan(
    config: RuntimeInstallConfig,
    *,
    python_executable: str | None = None,
    command: Sequence[str] | None = None,
    current_pythonpath: str | None = None,
) -> dict[str, object]:
    plan = {
        "install_mode": config.install_mode,
        "needs_install": config.needs_install,
        "package_spec": config.package_spec,
        "target_dir": str(Path(config.target_dir).resolve()),
        "index_url": config.index_url,
        "extra_index_url": config.extra_index_url,
        "extra_index_urls": list(config.all_extra_index_urls),
        "pip_args": list(config.pip_args),
        "install_command": build_runtime_install_command(config, python_executable=python_executable),
        "pythonpath": compute_runtime_pythonpath(config, current_pythonpath=current_pythonpath),
    }
    if command is not None:
        plan["exec_command"] = list(command)
    return plan


def install_runtime_package(
    config: RuntimeInstallConfig,
    *,
    python_executable: str | None = None,
    runner: Callable[..., object] = subprocess.run,
    clean_target: bool = True,
) -> list[str]:
    validate_runtime_install_config(config)
    if not config.needs_install:
        return []
    target_dir = Path(config.target_dir)
    if clean_target and target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    cmd = build_runtime_install_command(config, python_executable=python_executable)
    runner(cmd, check=True)
    return cmd


RunCallable = Callable[..., object]
ExecCallable = Callable[[str, Sequence[str], Mapping[str, str]], object]


def run_runtime_exec(
    command: Sequence[str],
    config: RuntimeInstallConfig,
    *,
    python_executable: str | None = None,
    env: Mapping[str, str] | None = None,
    runner: RunCallable = subprocess.run,
    execer: ExecCallable = os.execvpe,
) -> None:
    if not command:
        raise RuntimeInstallError("No command specified for dpone-runtime-exec")
    validate_runtime_install_config(config)
    if config.needs_install:
        install_runtime_package(config, python_executable=python_executable, runner=runner)
    exec_env = build_runtime_exec_env(config, env=env)
    execer(command[0], list(command), exec_env)


def main_install(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dpone-runtime-install",
        description="Install a dpone snapshot package into a writable runtime target directory.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print the installation plan and do not run pip install."
    )
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Plan output format for --dry-run.")
    args = parser.parse_args(argv)

    config = read_runtime_install_config()
    if args.dry_run:
        plan = build_runtime_install_plan(config)
        if args.format == "json":
            print(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(_format_plan_text(plan))
        return 0

    cmd = install_runtime_package(config)
    if not cmd:
        print("dpone runtime install: mode=baked, nothing to install")
        return 0
    print("dpone runtime install complete")
    return 0


def main_exec(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dpone-runtime-exec",
        description="Install a dpone snapshot package at startup (if configured) and exec the requested command.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print the install/exec plan and do not execute anything."
    )
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Plan output format for --dry-run.")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Command to exec, for example: airflow scheduler")
    args = parser.parse_args(argv)

    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]

    config = read_runtime_install_config()
    if args.dry_run:
        plan = build_runtime_install_plan(config, command=command, current_pythonpath=os.environ.get("PYTHONPATH"))
        if args.format == "json":
            print(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(_format_plan_text(plan))
        return 0

    run_runtime_exec(command, config)
    return 0


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _format_plan_text(plan: Mapping[str, object]) -> str:
    lines = [
        f"install_mode: {plan['install_mode']}",
        f"needs_install: {plan['needs_install']}",
        f"package_spec: {plan['package_spec']}",
        f"target_dir: {plan['target_dir']}",
        f"install_command: {shlex.join(plan['install_command']) if plan['install_command'] else '(none)'}",
        f"extra_index_urls: {plan['extra_index_urls']}",
        f"pythonpath: {plan['pythonpath']}",
    ]
    if plan.get("exec_command"):
        lines.append(f"exec_command: {shlex.join(plan['exec_command'])}")
    return "\n".join(lines)


__all__ = [
    "DEFAULT_INSTALL_MODE",
    "DEFAULT_PACKAGE_TARGET",
    "RuntimeInstallConfig",
    "RuntimeInstallError",
    "build_runtime_exec_env",
    "build_runtime_install_command",
    "build_runtime_install_plan",
    "compute_runtime_pythonpath",
    "install_runtime_package",
    "main_exec",
    "main_install",
    "read_runtime_install_config",
    "run_runtime_exec",
    "validate_runtime_install_config",
]
