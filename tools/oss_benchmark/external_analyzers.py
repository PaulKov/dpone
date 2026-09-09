"""External analyzer execution and normalization for benchmark evidence."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from tools.oss_benchmark.config import ROOT
from tools.oss_benchmark.payload_utils import as_float, as_int, project_slug


@dataclass(frozen=True)
class AnalyzerProcessResult:
    """Portable subprocess result used by analyzer runners and tests."""

    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class AnalyzerSpec:
    """External analyzer command contract."""

    tool: str
    kind: str
    args_before_path: tuple[str, ...]
    parser: AnalyzerOutputParser


class AnalyzerOutputParser(Protocol):
    """Normalize one analyzer's stdout into benchmark metrics."""

    def parse(self, stdout: str) -> dict[str, int | float]:
        """Return normalized metrics or raise ValueError for invalid output."""


class SubprocessAnalyzerRunner:
    """Run analyzer commands through subprocess with a narrow injectable API."""

    def run(self, args: list[str], *, cwd: Path, timeout_seconds: int) -> AnalyzerProcessResult:
        result = subprocess.run(
            args,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
            env=_portable_analyzer_env(),
        )
        return AnalyzerProcessResult(exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)


class TokeiParser:
    """Parse `tokei --output json` totals."""

    def parse(self, stdout: str) -> dict[str, int | float]:
        total = json.loads(stdout).get("Total") or {}
        code = as_int(total.get("code"))
        comments = as_int(total.get("comments"))
        blanks = as_int(total.get("blanks"))
        return {"total_sloc": code, "total_lines": code + comments + blanks, "files": as_int(total.get("files"))}


class ClocParser:
    """Parse `cloc --json` totals."""

    def parse(self, stdout: str) -> dict[str, int | float]:
        total = json.loads(stdout).get("SUM") or {}
        code = as_int(total.get("code"))
        comments = as_int(total.get("comment"))
        blanks = as_int(total.get("blank"))
        return {"total_sloc": code, "total_lines": code + comments + blanks, "files": as_int(total.get("nFiles"))}


class RadonParser:
    """Parse `radon cc -j` complexity output."""

    def parse(self, stdout: str) -> dict[str, int | float]:
        data = json.loads(stdout)
        complexities: list[float] = []
        file_count = 0
        for blocks in data.values():
            if not isinstance(blocks, list):
                continue
            file_count += 1
            complexities.extend(as_float(block.get("complexity")) for block in blocks if isinstance(block, dict))
        return _complexity_metrics(complexities, file_count=file_count)


class LizardParser:
    """Parse `lizard --xml` complexity output."""

    def parse(self, stdout: str) -> dict[str, int | float]:
        root = ET.fromstring(stdout)
        complexities: list[float] = []
        for measure in root.iter("measure"):
            if measure.attrib.get("type") != "Function":
                continue
            ccn_index = _lizard_ccn_index(measure)
            for item in measure.findall("item"):
                attribute_value = _lizard_attribute_complexity(item)
                if attribute_value is not None:
                    complexities.append(attribute_value)
                    continue
                values = item.findall("value")
                if ccn_index is not None and ccn_index < len(values):
                    complexities.append(as_float(values[ccn_index].text))
        return _complexity_metrics(complexities, file_count=len(complexities))


def collect_external_analyzer_results(
    projects: list[dict[str, Any]],
    *,
    generated_at: str,
    previous_payload: dict[str, Any] | None = None,
    command_runner: SubprocessAnalyzerRunner | None = None,
    tool_resolver: Any = shutil.which,
    timeout_seconds: int = 60,
) -> list[dict[str, Any]]:
    """Execute available analyzers and preserve previous values as stale on failure."""

    runner = command_runner or SubprocessAnalyzerRunner()
    results: list[dict[str, Any]] = []
    for project in projects:
        if project.get("unavailable"):
            continue
        actual_path = _actual_project_path(project)
        public_path = _public_project_path(actual_path)
        for spec in _SPECS:
            executable = tool_resolver(spec.tool)
            public_args = [spec.tool, *spec.args_before_path, public_path]
            if not executable:
                results.append(_stale_or_unavailable(spec, project, public_args, generated_at, previous_payload))
                continue
            actual_args = [str(executable), *spec.args_before_path, str(actual_path)]
            results.append(_run_spec(spec, project, actual_args, public_args, generated_at, runner, timeout_seconds))
    return results


def _run_spec(
    spec: AnalyzerSpec,
    project: dict[str, Any],
    actual_args: list[str],
    public_args: list[str],
    generated_at: str,
    runner: SubprocessAnalyzerRunner,
    timeout_seconds: int,
) -> dict[str, Any]:
    try:
        result = runner.run(actual_args, cwd=_actual_project_path(project), timeout_seconds=timeout_seconds)
        if result.exit_code != 0:
            return _unavailable_result(spec, project, public_args, generated_at, result.stderr, result.exit_code)
        metrics = spec.parser.parse(result.stdout)
    except (OSError, subprocess.SubprocessError, ET.ParseError, ValueError, json.JSONDecodeError) as exc:
        return _unavailable_result(spec, project, public_args, generated_at, str(exc), None)
    return {
        "tool": spec.tool,
        "project": project_slug(project),
        "status": "fresh",
        "kind": spec.kind,
        "command": _command(public_args),
        "version": None,
        "exit_code": result.exit_code,
        "generated_at": generated_at,
        "last_updated_at": generated_at,
        "refresh_attempted_at": generated_at,
        "error": None,
        "metrics": metrics,
    }


def _portable_analyzer_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update({"LANG": "C", "LC_ALL": "C", "LC_CTYPE": "C"})
    return env


def _stale_or_unavailable(
    spec: AnalyzerSpec,
    project: dict[str, Any],
    public_args: list[str],
    generated_at: str,
    previous_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    previous = _previous_metrics(spec, project_slug(project), previous_payload)
    if previous:
        return {
            "tool": spec.tool,
            "project": project_slug(project),
            "status": "stale",
            "kind": spec.kind,
            "command": _command(public_args),
            "version": None,
            "exit_code": None,
            "generated_at": previous["last_updated_at"],
            "last_updated_at": previous["last_updated_at"],
            "refresh_attempted_at": generated_at,
            "error": f"{spec.tool} not installed; retained previous analyzer value",
            "metrics": previous["metrics"],
        }
    return _unavailable_result(spec, project, public_args, generated_at, f"{spec.tool} not installed", None)


def _unavailable_result(
    spec: AnalyzerSpec,
    project: dict[str, Any],
    public_args: list[str],
    generated_at: str,
    error: str,
    exit_code: int | None,
) -> dict[str, Any]:
    return {
        "tool": spec.tool,
        "project": project_slug(project),
        "status": "unavailable",
        "kind": spec.kind,
        "command": _command(public_args),
        "version": None,
        "exit_code": exit_code,
        "generated_at": generated_at,
        "last_updated_at": None,
        "refresh_attempted_at": generated_at,
        "error": error,
        "metrics": {},
    }


def _previous_metrics(
    spec: AnalyzerSpec,
    slug: str,
    previous_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    validation = (previous_payload or {}).get("independent_validation") or {}
    checks_by_kind = {
        "loc_sloc": validation.get("loc_sloc_cross_checks") or {},
        "complexity": validation.get("complexity_cross_checks") or {},
    }
    for check in checks_by_kind.get(spec.kind, {}).get(slug, []):
        if check.get("tool") != spec.tool:
            continue
        metrics = _metrics_from_check(spec, check)
        if metrics:
            return {"metrics": metrics, "last_updated_at": _last_updated_at(check, previous_payload)}
    return None


def _metrics_from_check(spec: AnalyzerSpec, check: dict[str, Any]) -> dict[str, Any]:
    if spec.kind == "loc_sloc" and check.get("external_sloc") is not None:
        return {
            "total_sloc": as_int(check.get("external_sloc")),
            "total_lines": as_int(check.get("external_lines")),
        }
    if spec.kind == "complexity" and check.get("external_avg_complexity") is not None:
        return {
            "avg_complexity": as_float(check.get("external_avg_complexity")),
            "max_complexity": as_float(check.get("external_max_complexity")),
            "files": as_int(check.get("files")),
        }
    return {}


def _last_updated_at(check: dict[str, Any], previous_payload: dict[str, Any] | None) -> str:
    return str(
        check.get("last_updated_at")
        or check.get("generated_at")
        or (previous_payload or {}).get("generated_at")
        or ((previous_payload or {}).get("run_context") or {}).get("generated_at")
        or ""
    )


def _actual_project_path(project: dict[str, Any]) -> Path:
    return Path(str((project.get("spec") or {}).get("path") or ".")).resolve()


def _public_project_path(path: Path) -> str:
    try:
        relative = path.relative_to(ROOT.resolve())
    except ValueError:
        return f"$WORKSPACE/{path.name}" if path.is_absolute() else path.as_posix()
    relative_path = relative.as_posix()
    return "$WORKSPACE" if relative_path == "." else f"$WORKSPACE/{relative_path}"


def _command(args: list[str]) -> str:
    return " ".join(args)


def _complexity_metrics(complexities: list[float], *, file_count: int) -> dict[str, int | float]:
    if not complexities:
        return {"avg_complexity": 0.0, "max_complexity": 0.0, "files": file_count}
    return {
        "avg_complexity": round(sum(complexities) / len(complexities), 2),
        "max_complexity": max(complexities),
        "files": file_count,
    }


def _lizard_attribute_complexity(element: ET.Element) -> float | None:
    for attribute in ("cyclomatic_complexity", "cyclomatic", "ccn"):
        if element.attrib.get(attribute) is not None:
            return as_float(element.attrib.get(attribute))
    return None


def _lizard_ccn_index(measure: ET.Element) -> int | None:
    labels = [
        (label.text or "").strip().lower()
        for labels_element in measure.findall("labels")
        for label in labels_element.findall("label")
    ]
    for candidate in ("ccn", "cyclomatic_complexity", "cyclomatic"):
        if candidate in labels:
            return labels.index(candidate)
    return None


_SPECS = (
    AnalyzerSpec(
        "tokei",
        "loc_sloc",
        (
            "--exclude",
            ".cache",
            "--exclude",
            ".venv",
            "--exclude",
            "node_modules",
            "--exclude",
            "target",
            "--exclude",
            "dist",
            "--output",
            "json",
        ),
        TokeiParser(),
    ),
    AnalyzerSpec(
        "cloc",
        "loc_sloc",
        ("--json", "--exclude-dir=.git,.venv,.cache,node_modules,target,build,dist,__pycache__"),
        ClocParser(),
    ),
    AnalyzerSpec("radon", "complexity", ("cc", "-j"), RadonParser()),
    AnalyzerSpec("lizard", "complexity", ("--xml",), LizardParser()),
)
