"""Fail-closed policy evaluation for temporary sample runs."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.authoring import (
    AuthoringCompilationError,
    AuthoringCompiler,
    AuthoringSourceDependency,
    default_authoring_compiler,
)
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.services.safe_sample_capabilities import (
    SampleSourceCapabilities,
    SourceSamplingCapabilityDetector,
)
from dpone.services.sample_route_certifications import certified_sampling_routes


class SampleTarget(Enum):
    TEMPORARY = "temporary"


@dataclass(frozen=True, slots=True)
class SampleRunRequest:
    sample_rows: int
    target: SampleTarget
    environment: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_rows": self.sample_rows,
            "target": self.target.value,
            "environment": self.environment,
        }


@dataclass(frozen=True, slots=True)
class SafeSamplePolicy:
    environment: str
    require_pushdown: bool
    allow_full_scan: bool
    max_bytes: int
    timeout_seconds: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SafeSamplePolicyResult:
    passed: bool
    request: SampleRunRequest
    policy: SafeSamplePolicy
    capabilities: SampleSourceCapabilities
    errors: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "request": self.request.to_dict(),
            "policy": self.policy.to_dict(),
            "capabilities": self.capabilities.to_dict(),
            "errors": list(self.errors),
        }


class SafeSamplePlanError(RuntimeError):
    def __init__(self, code: str, message: str, *, path: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.path = path


@dataclass(frozen=True, slots=True)
class PipelineSourceSnapshot:
    path: Path
    payload: dict[str, Any]
    sha256: str
    dependencies: tuple[AuthoringSourceDependency, ...] = ()


@dataclass(frozen=True, slots=True)
class TemporaryTargetPlan:
    mode: str
    pipeline_id: str
    process: str
    sink_type: str
    connection_ref: str
    original_table: dict[str, str]
    temporary_table: dict[str, str]
    ttl_seconds: int
    cleanup_required: bool
    pii_policy: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SafeSamplePolicySet:
    """Environment-aware defaults from the frozen Airflow self-service contract."""

    def __init__(self, policies: dict[str, SafeSamplePolicy]) -> None:
        self._policies = dict(policies)

    @classmethod
    def default(cls) -> SafeSamplePolicySet:
        return cls(
            {
                "production": _production_policy("production"),
                "prod": _production_policy("prod"),
                "development": _development_policy("development"),
                "dev": _development_policy("dev"),
            }
        )

    def for_environment(self, environment: str) -> SafeSamplePolicy:
        key = environment.strip().lower() or "development"
        return self._policies.get(key) or SafeSamplePolicy(
            environment=key,
            require_pushdown=True,
            allow_full_scan=False,
            max_bytes=1024**3,
            timeout_seconds=60,
        )


def _production_policy(environment: str) -> SafeSamplePolicy:
    return SafeSamplePolicy(
        environment, require_pushdown=True, allow_full_scan=False, max_bytes=1024**3, timeout_seconds=60
    )


def _development_policy(environment: str) -> SafeSamplePolicy:
    return SafeSamplePolicy(
        environment,
        require_pushdown=False,
        allow_full_scan=True,
        max_bytes=10 * 1024**3,
        timeout_seconds=300,
    )


class TemporaryTargetPlanner:
    """Build a secret-free ephemeral target plan before sample execution."""

    def plan(
        self,
        *,
        pipeline_source: dict[str, Any],
        environment: str,
        run_id: str,
        process_selector: str | None = None,
        ttl_seconds: int = 24 * 60 * 60,
    ) -> TemporaryTargetPlan:
        try:
            pipeline_id = str(PipelineId.parse(_pipeline_id(pipeline_source)))
        except PipelineIdError as exc:
            raise SafeSamplePlanError(
                "DPONE_PIPELINE_ID_INVALID",
                "Pipeline metadata.id must use the canonical pipeline identity format.",
            ) from exc
        process = _selected_process(pipeline_source, process_selector=process_selector)
        sink = process.get("sink")
        if not isinstance(sink, dict):
            raise SafeSamplePlanError("DPONE_RUNTIME_TEMPORARY_TARGET_INVALID", "process sink must be a mapping")
        sink_type = str(sink.get("type") or "")
        connection_ref = str(sink.get("connection_ref") or "")
        if not sink_type or not connection_ref:
            raise SafeSamplePlanError(
                "DPONE_RUNTIME_TEMPORARY_TARGET_INVALID",
                "sink type and connection_ref are required",
            )
        original_table = _table_dict(sink.get("table"))
        temporary_schema = f"dpone_tmp_{_safe_id(environment.lower() or 'development')}"
        physical_pipeline_id = _safe_id(pipeline_id)
        temporary_name = f"{physical_pipeline_id}_{_stable_suffix(pipeline_id, process, environment, run_id)}"
        return TemporaryTargetPlan(
            mode="temporary",
            pipeline_id=pipeline_id,
            process=str(process.get("name") or pipeline_id),
            sink_type=sink_type,
            connection_ref=connection_ref,
            original_table=original_table,
            temporary_table={"schema": temporary_schema, "name": temporary_name},
            ttl_seconds=ttl_seconds,
            cleanup_required=True,
            pii_policy="masked",
        )


def build_temporary_target_plan_from_file(
    path: str | Path,
    *,
    environment: str,
    run_id: str,
    process_selector: str | None = None,
) -> TemporaryTargetPlan:
    return TemporaryTargetPlanner().plan(
        pipeline_source=load_pipeline_source_from_file(path),
        environment=environment,
        run_id=run_id,
        process_selector=process_selector,
    )


def detect_source_sampling_capabilities_from_file(
    path: str | Path,
    *,
    verified_route_ids: Iterable[str] = (),
    authoring_compiler: AuthoringCompiler | None = None,
    process_selector: str | None = None,
) -> SampleSourceCapabilities:
    try:
        payload = load_pipeline_source_from_file(path, authoring_compiler=authoring_compiler)
    except SafeSamplePlanError:
        return SampleSourceCapabilities.unknown()
    return detect_source_sampling_capabilities(
        payload,
        verified_route_ids=verified_route_ids,
        process_selector=process_selector,
    )


def detect_source_sampling_capabilities(
    pipeline_source: dict[str, Any],
    *,
    verified_route_ids: Iterable[str] = (),
    process_selector: str | None = None,
) -> SampleSourceCapabilities:
    """Detect capabilities from one already compiled source snapshot."""

    try:
        process = _selected_process(pipeline_source, process_selector=process_selector)
    except SafeSamplePlanError:
        return SampleSourceCapabilities.unknown()
    return SourceSamplingCapabilityDetector(verified_route_ids=verified_route_ids).detect(
        {**pipeline_source, "processes": [process]}
    )


def load_pipeline_source_from_file(
    path: str | Path,
    *,
    authoring_compiler: AuthoringCompiler | None = None,
) -> dict[str, Any]:
    return load_pipeline_source_snapshot_from_file(path, authoring_compiler=authoring_compiler).payload


def load_pipeline_source_snapshot_from_file(
    path: str | Path,
    *,
    source_reader: Callable[[Path], bytes] | None = None,
    authoring_compiler: AuthoringCompiler | None = None,
) -> PipelineSourceSnapshot:
    """Read and parse a pipeline once while retaining its exact content digest."""

    source_path = _resolve_pipeline_path(Path(path))
    if not source_path.exists():
        raise SafeSamplePlanError(
            "DPONE_PIPELINE_SOURCE_NOT_FOUND",
            "pipeline source was not found",
            path=source_path.as_posix(),
        )
    try:
        source_bytes = source_reader(source_path) if source_reader is not None else source_path.read_bytes()
        payload = yaml.safe_load(source_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise SafeSamplePlanError(
            "DPONE_PIPELINE_SOURCE_INVALID",
            "pipeline source could not be read as UTF-8 YAML",
            path=source_path.as_posix(),
        ) from exc
    if not isinstance(payload, dict):
        raise SafeSamplePlanError(
            "DPONE_PIPELINE_SOURCE_INVALID",
            "pipeline source must be a YAML object",
            path=source_path.as_posix(),
        )
    try:
        compiler = authoring_compiler or default_authoring_compiler()
        compilation = compiler.compile(payload, source_path=source_path)
    except (AuthoringCompilationError, ManifestConfigurationError) as exc:
        code = exc.code if isinstance(exc, AuthoringCompilationError) else "DPONE_AUTHORING_COMPILATION_FAILED"
        raise SafeSamplePlanError(code, str(exc), path=source_path.as_posix()) from exc
    process_view = {**payload, "processes": [dict(process) for process in compilation.processes]}
    return PipelineSourceSnapshot(
        path=source_path.resolve(strict=False),
        payload=process_view,
        sha256="sha256:" + hashlib.sha256(source_bytes).hexdigest(),
        dependencies=compilation.dependencies,
    )


class SafeSamplePolicyEvaluator:
    """Evaluate sample-run safety without touching source systems or secrets."""

    def evaluate(
        self,
        request: SampleRunRequest,
        policy: SafeSamplePolicy,
        capabilities: SampleSourceCapabilities,
    ) -> SafeSamplePolicyResult:
        errors: list[dict[str, Any]] = []
        if request.sample_rows <= 0:
            errors.append(_error("DPONE_RUNTIME_SAMPLE_SIZE_INVALID", "Sample row budget must be greater than zero."))
        if request.target is not SampleTarget.TEMPORARY:
            errors.append(_error("DPONE_SECURITY_SAMPLE_TARGET_UNSAFE", "Sample runs may only use temporary targets."))
        if policy.require_pushdown and not _is_proven_pushdown(capabilities, policy):
            errors.append(
                _error(
                    "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED",
                    "Production sample policy requires connector-proven pushdown sampling.",
                )
            )
        if capabilities.full_scan_required and not policy.allow_full_scan:
            errors.append(
                _error("DPONE_SECURITY_SAMPLE_FULL_SCAN_FORBIDDEN", "Full-scan sampling is forbidden by policy.")
            )
        if capabilities.full_scan_required and (
            capabilities.estimated_read_bytes is None or capabilities.estimated_read_bytes <= 0
        ):
            errors.append(
                _error(
                    "DPONE_SECURITY_SAMPLE_BUDGET_REQUIRED",
                    "Full-scan sampling requires an estimated source read byte budget.",
                )
            )
        if capabilities.estimated_read_bytes is not None and capabilities.estimated_read_bytes > policy.max_bytes:
            errors.append(
                _error(
                    "DPONE_SECURITY_SAMPLE_BUDGET_EXCEEDED",
                    "Estimated source read bytes exceed the sample policy budget.",
                )
            )
        return SafeSamplePolicyResult(
            passed=not errors,
            request=request,
            policy=policy,
            capabilities=capabilities,
            errors=tuple(errors),
        )


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_policy",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


def _resolve_pipeline_path(path: Path) -> Path:
    return path if path.suffix in {".yaml", ".yml"} or path.name == "pipeline.yaml" else path / "pipeline.yaml"


def _pipeline_id(source: dict[str, Any]) -> str:
    metadata = source.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("id"), str):
        return metadata["id"]
    return ""


def _selected_process(source: dict[str, Any], *, process_selector: str | None) -> dict[str, Any]:
    processes = source.get("processes")
    if not isinstance(processes, list) or not processes or not all(isinstance(item, dict) for item in processes):
        raise SafeSamplePlanError("DPONE_PIPELINE_PROCESS_MISSING", "processes must contain at least one process")
    typed_processes: list[dict[str, Any]] = processes
    if process_selector:
        matches = [
            process
            for process in typed_processes
            if process_selector in {str(process.get("name") or ""), str(process.get("selector") or "")}
        ]
        if len(matches) != 1:
            raise SafeSamplePlanError(
                "DPONE_PIPELINE_PROCESS_NOT_FOUND",
                "The requested process selector does not identify exactly one process.",
            )
        return matches[0]
    if len(typed_processes) != 1:
        raise SafeSamplePlanError(
            "DPONE_PIPELINE_PROCESS_AMBIGUOUS",
            "A multi-process pipeline requires --selector for a safe sample run.",
        )
    return typed_processes[0]


def _is_proven_pushdown(capabilities: SampleSourceCapabilities, policy: SafeSamplePolicy) -> bool:
    if capabilities.supports_pushdown_sampling is not True:
        return False
    if not capabilities.proof:
        return False
    normalized = capabilities.proof.strip().lower()
    if policy.require_pushdown and policy.environment in {"production", "prod"}:
        return capabilities.production_proof_verified and normalized.startswith("route_certification:")
    return normalized == "connector_capability" or normalized.startswith(
        ("connector_capability:", "route_certification:")
    )


def _table_dict(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {"schema": "", "name": ""}
    return {"schema": str(value.get("schema") or ""), "name": str(value.get("name") or "")}


def _stable_suffix(pipeline_id: str, process: dict[str, Any], environment: str, run_id: str) -> str:
    raw = "|".join([pipeline_id, str(process.get("name") or ""), environment, run_id])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _safe_id(value: str) -> str:
    normalized = "".join(char if char.isalnum() or char == "_" else "_" for char in value.strip())
    return normalized.strip("_") or "sample"


__all__ = [
    "SafeSamplePlanError",
    "PipelineSourceSnapshot",
    "SafeSamplePolicy",
    "SafeSamplePolicyEvaluator",
    "SafeSamplePolicyResult",
    "SafeSamplePolicySet",
    "SampleRunRequest",
    "SampleSourceCapabilities",
    "SampleTarget",
    "SourceSamplingCapabilityDetector",
    "TemporaryTargetPlan",
    "TemporaryTargetPlanner",
    "certified_sampling_routes",
    "detect_source_sampling_capabilities",
    "detect_source_sampling_capabilities_from_file",
    "build_temporary_target_plan_from_file",
    "load_pipeline_source_from_file",
    "load_pipeline_source_snapshot_from_file",
]
