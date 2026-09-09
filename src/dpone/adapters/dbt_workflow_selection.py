"""Manifest-preview and dbt-CLI implementations of workflow selection."""

from __future__ import annotations

import importlib.metadata
import subprocess
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from dpone.adapters.dbt_executable import current_environment_dbt_executable
from dpone.adapters.dbt_sqlserver_graph_policy import MAX_DBT_SELECTION_OUTPUT_BYTES
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_selection import DbtSelectionPlan, ResolvedDbtSelection, manifest_preview_selected_graph
from dpone.contracts.dbt_toolchain import certified_adapter_distribution
from dpone.contracts.strict_json import StrictJsonError, strict_json_object

_EXECUTABLE_PREFIXES = ("model.", "seed.", "snapshot.", "test.", "unit_test.")

if TYPE_CHECKING:
    from dpone.ports.dbt_selection import DbtParseTargetResolver


class ManifestPreviewSelectionResolver:
    """Resolve closure from dbt's generated parent/child maps for local preview."""

    def resolve(
        self,
        *,
        project_root: Path,
        manifest_bytes: bytes,
        selected_unique_ids: tuple[str, ...],
        profiles_dir: Path | None,
        profile_name: str,
        target_name: str,
        dbt_core_version: str,
        dbt_adapter: str,
        dbt_adapter_version: str,
    ) -> ResolvedDbtSelection:
        del (
            project_root,
            profiles_dir,
            profile_name,
            target_name,
            dbt_core_version,
            dbt_adapter,
            dbt_adapter_version,
        )
        plan = DbtSelectionPlan.from_manifest(manifest_bytes, selected_unique_ids)
        return plan.complete(
            plan.manifest,
            manifest_preview_selected_graph(plan.manifest, selected_unique_ids),
            authority="manifest_preview",
        )


class DbtCliSelectionResolver:
    """Ask the exact installed dbt toolchain for the selected executable graph."""

    def __init__(
        self,
        *,
        runner: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
        package_version: Callable[[str], str] = importlib.metadata.version,
        dbt_executable: str | None = None,
        target_resolver: DbtParseTargetResolver | None = None,
    ) -> None:
        self._runner = runner
        self._package_version = package_version
        self._dbt_executable = dbt_executable or current_environment_dbt_executable()
        self._target_resolver = target_resolver

    def resolve(
        self,
        *,
        project_root: Path,
        manifest_bytes: bytes,
        selected_unique_ids: tuple[str, ...],
        profiles_dir: Path | None,
        profile_name: str,
        target_name: str,
        dbt_core_version: str,
        dbt_adapter: str,
        dbt_adapter_version: str,
    ) -> ResolvedDbtSelection:
        _require_token(profile_name, "dbt profile")
        _require_token(target_name, "dbt target")
        if self._target_resolver is not None and (
            profiles_dir is None
            or Path(self._dbt_executable).absolute() != Path(current_environment_dbt_executable()).absolute()
        ):
            raise ValueError("rendered target requires captured profiles and the certified active dbt executable")
        _require_version(self._package_version, "dbt-core", dbt_core_version)
        _require_version(
            self._package_version,
            certified_adapter_distribution(dbt_adapter),
            dbt_adapter_version,
        )
        plan = DbtSelectionPlan.from_manifest(manifest_bytes, selected_unique_ids)
        selectors = plan.selectors
        with TemporaryDirectory(prefix="dpone-dbt-selection-") as temporary:
            state_root = Path(temporary)
            invocation = DbtInvocationContext.canonical()
            invocation_home = state_root / "home"
            invocation_home.mkdir(mode=0o700)
            common = [
                self._dbt_executable,
                "--quiet",
                "--no-use-colors",
            ]
            location = [
                "--project-dir",
                str(project_root),
                "--profile",
                profile_name,
                "--target",
                target_name,
                "--target-path",
                str(state_root / "target"),
                "--log-path",
                str(state_root / "logs"),
            ]
            if profiles_dir is not None:
                location[4:4] = ["--profiles-dir", str(profiles_dir)]
            parse_argv = [*common, "parse", *location]
            parse_argv.extend(("--vars", invocation.selection_vars_json()))
            invocation_target = (
                self._target_resolver.resolve(
                    project_root=project_root,
                    profiles_dir=profiles_dir,
                    parse_args=tuple(parse_argv[4:]),
                    environment=invocation.environment(home=str(invocation_home)),
                )
                if self._target_resolver is not None and profiles_dir is not None
                else None
            )
            parsed = self._runner(
                tuple(parse_argv),
                cwd=project_root,
                check=False,
                shell=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
                env=invocation.environment(home=str(invocation_home)),
            )
            if parsed.returncode != 0:
                raise ValueError("dbt parse failed for the captured project snapshot")
            parsed_manifest = _bounded_file(
                state_root / "target" / "manifest.json",
                maximum=MAX_DBT_SELECTION_OUTPUT_BYTES,
            )
            plan.require_matching_manifest(parsed_manifest)
            argv = [
                *common,
                "ls",
                *location,
                "--resource-type",
                "model",
                "seed",
                "snapshot",
                "test",
                "unit_test",
                "--indirect-selection",
                invocation.indirect_selection,
                "--output",
                "json",
                "--output-keys",
                "unique_id",
                "--select",
                *selectors,
                "--vars",
                invocation.selection_vars_json(),
            ]
            completed = self._runner(
                tuple(argv),
                cwd=project_root,
                check=False,
                shell=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
                env=invocation.environment(home=str(invocation_home)),
            )
        if completed.returncode != 0:
            raise ValueError("dbt selection command failed")
        if len(completed.stdout) > MAX_DBT_SELECTION_OUTPUT_BYTES:
            raise ValueError("dbt selection output exceeds the bounded limit")
        selected_graph = tuple(sorted(_selection_ids(completed.stdout)))
        parsed_manifest_object = _json_object(parsed_manifest)
        return plan.complete(
            parsed_manifest_object,
            selected_graph,
            authority="dbt_cli",
            invocation_target=invocation_target,
        )


def _selection_ids(payload: bytes) -> set[str]:
    result: set[str] = set()
    for raw_line in payload.splitlines():
        if not raw_line.strip():
            continue
        item = _json_object(raw_line)
        unique_id = item.get("unique_id")
        if not isinstance(unique_id, str) or not unique_id.startswith(_EXECUTABLE_PREFIXES):
            raise ValueError("dbt selection output contains an invalid unique_id")
        result.add(unique_id)
    if not result:
        raise ValueError("dbt selection returned no executable nodes")
    return result


def _bounded_file(path: Path, *, maximum: int) -> bytes:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
            raise ValueError("dbt parse manifest is missing, unsafe, or oversized")
        return path.read_bytes()
    except OSError as exc:
        raise ValueError("dbt parse manifest could not be read") from exc


def _require_version(
    provider: Callable[[str], str],
    distribution: str,
    expected: str,
) -> None:
    try:
        actual = provider(distribution)
    except importlib.metadata.PackageNotFoundError as exc:
        raise ValueError(f"required dbt package is not installed: {distribution}") from exc
    if actual != expected:
        raise ValueError(f"{distribution} version does not match the certified lock")


def _require_token(value: str, label: str) -> None:
    if not isinstance(value, str) or not value.strip() or any(character.isspace() for character in value):
        raise ValueError(f"{label} must be a non-blank token")


def _json_object(payload: bytes) -> dict[str, Any]:
    try:
        return strict_json_object(payload)
    except StrictJsonError as exc:
        raise ValueError("dbt selection input must be a strict JSON object") from exc


__all__ = [
    "DbtCliSelectionResolver",
    "ManifestPreviewSelectionResolver",
]
