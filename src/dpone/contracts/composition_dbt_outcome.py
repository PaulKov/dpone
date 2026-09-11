"""Supervisor dispatch/capture values; only protected persistence grants provenance."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import PurePosixPath

from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.strict_json import canonical_json_bytes

ARTIFACT_ROLES = ("preflight_manifest", "build_manifest", "run_results", "execution_evidence")
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
EVIDENCE_SUBJECT_FIELDS = frozenset(
    {
        "release_id",
        "deployment_id",
        "workload_pack_sha256",
        "project_bundle_sha256",
        "manifest_sha256",
        "selection_sha256",
        "toolchain_sha256",
        "invocation_context_sha256",
        "logical_target_sha256",
        "target_binding_sha256",
        "adapter_policy_sha256",
        "graph_policy_sha256",
        "workflow_id",
    }
)


class DbtCaptureError(RuntimeError):
    """Sanitized capture failure; it never implies SQL rolled back."""


def _text(value: object) -> None:
    if type(value) is not str or not value or "\x00" in value:
        raise DbtCaptureError("capture_identity")


@dataclass(frozen=True)
class DbtDispatchIntent:
    """Exact app-derived build command after verified preflight; no credentials.

    Root and paths come from protected admission, never user arguments supplied
    to dispatch. Directories must be preprovisioned; capture never chmods them.
    The protected writer retains original preflight bytes with this intent.
    """

    attempt: CompositionAttemptIdentity
    argv: tuple[str, ...]
    toolchain_sha256: str
    sql_principal_sid: str
    output_directory: str
    supervisor_uid: int
    child_uid: int
    child_gid: int
    artifact_paths: tuple[tuple[str, str], ...]
    preflight_manifest_sha256: str

    def __post_init__(self) -> None:
        if type(self.attempt) is not CompositionAttemptIdentity:
            raise DbtCaptureError("capture_attempt")
        self.attempt.__post_init__()
        if type(self.argv) is not tuple or not self.argv or len(self.argv) > 256:
            raise DbtCaptureError("capture_argv")
        for value in (*self.argv, self.toolchain_sha256, self.sql_principal_sid, self.output_directory):
            _text(value)
        if any(
            re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
            for value in (self.toolchain_sha256, self.preflight_manifest_sha256)
        ):
            raise DbtCaptureError("capture_digest")
        if re.fullmatch(r"mssql-sid:(?:[0-9a-f]{2}){1,85}", self.sql_principal_sid) is None:
            raise DbtCaptureError("capture_sid")
        if not PurePosixPath(self.argv[0]).is_absolute():
            raise DbtCaptureError("capture_argv")
        root = PurePosixPath(self.output_directory)
        if not root.is_absolute() or ".." in root.parts or str(root) != self.output_directory:
            raise DbtCaptureError("capture_path")
        if any(type(uid) is not int or uid < 0 for uid in (self.supervisor_uid, self.child_uid, self.child_gid)):
            raise DbtCaptureError("capture_uid")
        if type(self.artifact_paths) is not tuple or tuple(role for role, _ in self.artifact_paths) != ARTIFACT_ROLES:
            raise DbtCaptureError("capture_artifacts")
        names = []
        for role, name in self.artifact_paths:
            _text(name)
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts or str(path) != name or name == ".":
                raise DbtCaptureError("capture_path")
            names.append(name)
        if len(set(names)) != len(names):
            raise DbtCaptureError("capture_artifacts")

    @property
    def intent_sha256(self) -> str:
        return "sha256:" + sha256(canonical_json_bytes(asdict(self))).hexdigest()


@dataclass(frozen=True)
class DbtArtifactOriginal:
    role: str
    relative_path: str
    content: bytes

    def __post_init__(self) -> None:
        if (
            self.role not in ARTIFACT_ROLES
            or type(self.content) is not bytes
            or not 0 < len(self.content) <= MAX_ARTIFACT_BYTES
        ):
            raise DbtCaptureError("capture_artifact")

    @property
    def sha256(self) -> str:
        return "sha256:" + sha256(self.content).hexdigest()


@dataclass(frozen=True)
class DbtChildExit:
    """Supervisor-observed process identity and wait result, not SQL outcome."""

    pid: int
    start_ticks: int
    exit_code: int

    def __post_init__(self) -> None:
        if (
            any(type(value) is not int for value in (self.pid, self.start_ticks, self.exit_code))
            or min(self.pid, self.start_ticks) <= 0
        ):
            raise DbtCaptureError("capture_process")


@dataclass(frozen=True)
class DbtExitRecord:
    intent: DbtDispatchIntent
    child: DbtChildExit
    quiescence_original: bytes
    preflight_original: DbtArtifactOriginal


@dataclass(frozen=True)
class DbtCaptureRecord:
    """Written/read only through protected supervisor persistence.

    UNDISPATCHED is allowed only from a protected controller record proving no
    build intent was accepted and complete issued-connection/process closure.
    After any accepted intent, failure or ambiguity remains COMMIT_UNKNOWN.
    """

    intent: DbtDispatchIntent
    phase: str
    exit_record: DbtExitRecord | None = None
    originals: tuple[DbtArtifactOriginal, ...] = ()
    undispatched_closure_original: bytes | None = None


@dataclass(frozen=True)
class DbtOutcomeExpectation:
    """Immutable app-derived selected graph and execution-evidence subject."""

    graph_contract_sha256: str
    selected_graph_unique_ids: tuple[str, ...]
    expected_run_result_unique_ids: tuple[str, ...]
    dbt_core_version: str
    manifest_schema_version: str
    run_results_schema_version: str
    evidence_subject: tuple[tuple[str, str], ...]
    materializations: tuple[tuple[str, str, str], ...]
    warning_policy: str = "fail"

    def __post_init__(self) -> None:
        for values in (self.selected_graph_unique_ids, self.expected_run_result_unique_ids):
            if type(values) is not tuple or not values or len(set(values)) != len(values):
                raise DbtCaptureError("capture_selection")
            for value in values:
                _text(value)
        if not set(self.expected_run_result_unique_ids) <= set(self.selected_graph_unique_ids):
            raise DbtCaptureError("capture_selection")
        if (
            self.manifest_schema_version not in {"v10", "v11", "v12"}
            or self.run_results_schema_version != "v6"
            or self.warning_policy not in {"allow", "fail"}
        ):
            raise DbtCaptureError("capture_toolchain")
        _text(self.dbt_core_version)
        if (
            type(self.evidence_subject) is not tuple
            or len(self.evidence_subject) != len(EVIDENCE_SUBJECT_FIELDS)
            or {key for key, _ in self.evidence_subject} != EVIDENCE_SUBJECT_FIELDS
        ):
            raise DbtCaptureError("capture_evidence_subject")
        if type(self.materializations) is not tuple:
            raise DbtCaptureError("materialization_expectation")
        for unique_id, kind, schema_digest in self.materializations:
            _text(unique_id)
            if kind not in {"table", "view"} or re.fullmatch(r"sha256:[0-9a-f]{64}", schema_digest) is None:
                raise DbtCaptureError("materialization_expectation")


@dataclass(frozen=True)
class DbtNativeOutcome:
    state: str
    reason: str
    intent_sha256: str
    materialization_original: bytes | None = None
