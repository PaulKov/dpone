from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
    from dpone.gitops.workload_dependencies import WorkloadFileDependency


import base64
import gzip
import hashlib
import io
import tarfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.manifest.confined_files import read_confined_file

WORKLOAD_BOOTSTRAP_KEY = "__workload__"
DEFAULT_PROCESS_BOOTSTRAP_KEY = "__default_process__"
_RESERVED_BOOTSTRAP_KEYS = frozenset(
    {
        WORKLOAD_BOOTSTRAP_KEY,
        DEFAULT_PROCESS_BOOTSTRAP_KEY,
    }
)


class InlineWorkloadConfigurationError(ValueError):
    """An inline workload archive or selector violates its public contract."""


class InlineWorkloadDependencyError(ValueError):
    """Archived bytes no longer match the dependency closure resolved for the pack."""

    code = "workload_dependency_changed_during_bootstrap"

    def __init__(self, path: str) -> None:
        super().__init__(self.code)
        self.path = path


@dataclass(frozen=True, slots=True)
class RuntimeWorkloadPayload:
    """Canonical compressed workload archive transported by a strict pack."""

    data: str
    sha256: str
    bytes: int

    def to_jsonable(self) -> dict[str, object]:
        return {
            "schema": "dpone.airflow-runtime-payload.v1",
            "archive": {
                "encoding": "base64",
                "format": "tar+gzip",
                "sha256": self.sha256,
                "bytes": self.bytes,
                "data": self.data,
            },
        }


class RuntimePayloadBuilder:
    """Read and compress one dependency closure at most once."""

    def __init__(
        self,
        *,
        repo_root: Path,
        paths: tuple[str, ...],
        expected_sha256_by_path: Mapping[str, str] | None = None,
        generated_files: Mapping[str, bytes] | None = None,
    ) -> None:
        self._repo_root = repo_root
        self._paths = paths
        self._expected_sha256_by_path = dict(expected_sha256_by_path or {})
        self._generated_files = dict(generated_files or {})
        self._payload: RuntimeWorkloadPayload | None = None

    def build(self) -> RuntimeWorkloadPayload:
        if self._payload is None:
            compressed = _inline_workload_archive_bytes(
                repo_root=self._repo_root,
                paths=self._paths,
                expected_sha256_by_path=self._expected_sha256_by_path,
                generated_files=self._generated_files,
            )
            self._payload = RuntimeWorkloadPayload(
                data=base64.b64encode(compressed).decode("ascii"),
                sha256="sha256:" + hashlib.sha256(compressed).hexdigest(),
                bytes=len(compressed),
            )
        return self._payload


def runtime_workload_bootstrap(
    *,
    runtime_manifest_path: str,
    workload_id: str,
    process_selectors: tuple[str | None, ...] = (),
) -> dict[str, object]:
    """Build shell-free runtime commands selected from a verified pack.

    Keep the workload-id entry for v1/v2 rollback and emit ``__workload__``
    without a selector for explicit v3 whole-workload execution. Process-scoped
    v3 plans select the exact process key or ``__default_process__``.
    """

    manifest_path = _safe_archive_path(runtime_manifest_path)
    workload_key = _runtime_selector_key(workload_id)
    if workload_key in _RESERVED_BOOTSTRAP_KEYS:
        raise InlineWorkloadConfigurationError("runtime_bootstrap_selector_reserved")
    commands: dict[str, object] = {
        workload_key: {
            "argv": [
                "dpone",
                "run",
                manifest_path,
                "--format",
                "json",
                "--selector",
                workload_key,
            ],
            "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
        },
        WORKLOAD_BOOTSTRAP_KEY: {
            "argv": [
                "dpone",
                "run",
                manifest_path,
                "--format",
                "json",
            ],
            "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
        },
    }
    for selector in process_selectors:
        key = DEFAULT_PROCESS_BOOTSTRAP_KEY if selector is None else _runtime_selector_key(selector)
        if key in _RESERVED_BOOTSTRAP_KEYS and selector is not None:
            raise InlineWorkloadConfigurationError("runtime_bootstrap_selector_reserved")
        if key in commands:
            continue
        argv = ["dpone", "run", manifest_path, "--format", "json"]
        if selector is not None:
            argv.extend(("--selector", selector))
        commands[key] = {
            "argv": argv,
            "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
        }
    return {
        "schema": "dpone.airflow-runtime-bootstrap.v1",
        "commands": commands,
    }


def inline_workload_bootstrap(
    *,
    workload: GitOpsWorkloadDefinition,
    repo_root: Path,
    image: str,
    dependencies: tuple[WorkloadFileDependency, ...],
    runner_embed_paths: tuple[str, ...] = (),
    generated_files: Mapping[str, bytes] | None = None,
    payload_builder: RuntimePayloadBuilder | None = None,
) -> dict[str, object]:
    """Build initContainer spec that unpacks an inline workload tarball into the pod."""
    builder = payload_builder or runtime_workload_payload_builder(
        workload=workload,
        repo_root=repo_root,
        dependencies=dependencies,
        runner_embed_paths=runner_embed_paths,
        generated_files=generated_files,
    )
    archive = builder.build().data
    script = (
        "set -eu\n"
        "mkdir -p /workspace/repo\n"
        "base64 -d <<'DPONE_INLINE_WORKLOAD_TAR_EOF' | tar -xz -C /workspace/repo\n"
        f"{archive}\n"
        "DPONE_INLINE_WORKLOAD_TAR_EOF\n"
    )
    return {
        "name": "dpone-inline-workload-bootstrap",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/sh", "-ec"],
        "args": [script],
        "volumeMounts": [{"name": "dpone-worktree", "mountPath": "/workspace", "readOnly": False}],
    }


def runtime_workload_payload_builder(
    *,
    workload: GitOpsWorkloadDefinition,
    repo_root: Path,
    dependencies: tuple[WorkloadFileDependency, ...],
    runner_embed_paths: tuple[str, ...] = (),
    generated_files: Mapping[str, bytes] | None = None,
) -> RuntimePayloadBuilder:
    """Create the one-shot archive builder shared by both pack projections."""

    archive_paths = tuple(
        dict.fromkeys((workload.manifest, *(dependency.path for dependency in dependencies), *runner_embed_paths))
    )
    return RuntimePayloadBuilder(
        repo_root=repo_root,
        paths=archive_paths,
        expected_sha256_by_path=_dependency_digests(dependencies),
        generated_files=generated_files,
    )


def inline_workload_archive(
    *,
    repo_root: Path,
    paths: tuple[str, ...],
    expected_sha256_by_path: Mapping[str, str] | None = None,
    generated_files: Mapping[str, bytes] | None = None,
) -> str:
    """Return the legacy canonical base64 archive projection."""

    return (
        RuntimePayloadBuilder(
            repo_root=repo_root,
            paths=paths,
            expected_sha256_by_path=expected_sha256_by_path,
            generated_files=generated_files,
        )
        .build()
        .data
    )


def _inline_workload_archive_bytes(
    *,
    repo_root: Path,
    paths: tuple[str, ...],
    expected_sha256_by_path: Mapping[str, str] | None = None,
    generated_files: Mapping[str, bytes] | None = None,
) -> bytes:
    expected = {
        _safe_archive_path(path): digest.removeprefix("sha256:")
        for path, digest in (expected_sha256_by_path or {}).items()
    }
    raw = io.BytesIO()
    # ``tarfile.open(mode="w:gz")`` writes wall-clock time into the gzip
    # header. Keep both archive layers canonical so identical authoring input
    # produces identical pack and release fingerprints across processes.
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
            seen: set[str] = set()
            for rel in paths:
                safe_rel = _safe_archive_path(rel)
                if safe_rel in seen:
                    continue
                data = read_confined_file(repo_root, safe_rel, max_bytes=(1 << 63) - 1)
                expected_digest = expected.get(safe_rel)
                if expected_digest is not None and hashlib.sha256(data).hexdigest() != expected_digest:
                    raise InlineWorkloadDependencyError(safe_rel)
                seen.add(safe_rel)
                info = tarfile.TarInfo(safe_rel)
                info.size = len(data)
                info.mtime = 0
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(data))
            for rel, data in sorted((generated_files or {}).items()):
                safe_rel = _safe_archive_path(rel)
                if safe_rel in seen:
                    raise InlineWorkloadConfigurationError("generated_workload_path_conflict")
                seen.add(safe_rel)
                info = tarfile.TarInfo(safe_rel)
                info.size = len(data)
                info.mtime = 0
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(data))
            missing = set(expected) - seen
            if missing:
                raise InlineWorkloadDependencyError(sorted(missing)[0])
    return raw.getvalue()


def _dependency_digests(dependencies: tuple[WorkloadFileDependency, ...]) -> dict[str, str]:
    expected: dict[str, str] = {}
    for dependency in dependencies:
        path = _safe_archive_path(dependency.path)
        digest = dependency.sha256.removeprefix("sha256:")
        previous = expected.get(path)
        if previous is not None and previous != digest:
            raise InlineWorkloadDependencyError(path)
        expected[path] = digest
    return expected


def _safe_archive_path(raw: str) -> str:
    path = PurePosixPath(raw)
    if (
        not raw
        or "\\" in raw
        or path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise InlineWorkloadConfigurationError("inline_workload_path_unsafe")
    return path.as_posix()


def _runtime_selector_key(selector: str | None) -> str:
    if selector is None:
        return "__default__"
    if not isinstance(selector, str) or not selector or len(selector) > 256 or any(char.isspace() for char in selector):
        raise InlineWorkloadConfigurationError("runtime_bootstrap_selector_invalid")
    return selector


__all__ = [
    "DEFAULT_PROCESS_BOOTSTRAP_KEY",
    "InlineWorkloadConfigurationError",
    "InlineWorkloadDependencyError",
    "RuntimePayloadBuilder",
    "RuntimeWorkloadPayload",
    "inline_workload_archive",
    "inline_workload_bootstrap",
    "runtime_workload_payload_builder",
    "runtime_workload_bootstrap",
]
