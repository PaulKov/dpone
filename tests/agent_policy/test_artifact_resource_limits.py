from __future__ import annotations

import importlib.util
import io
import stat
import sys
import warnings
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


limits = _load(
    "dpone_agent_artifact_resource_limits_test",
    "tools/agent_policy/artifact_resource_limits.py",
)
gate = _load(
    "dpone_agent_release_merge_receipt_resource_test",
    "tools/agent_policy/release_merge_receipt_gate.py",
)
REPOSITORY = "PaulKov/dpone"
INTEGRATION_SHA = "d" * 40


class _ObservedStream:
    def __init__(self, content: bytes) -> None:
        self._stream = io.BytesIO(content)
        self.requested: list[int] = []
        self.returned: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.requested.append(size)
        content = self._stream.read(size)
        self.returned.append(len(content))
        return content


def _archive(members: list[tuple[str | zipfile.ZipInfo, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in members:
                archive.writestr(name, content)
    return buffer.getvalue()


def test_bounded_reader_accepts_exact_limit_and_never_requests_beyond_limit_plus_one() -> None:
    stream = _ObservedStream(b"12345")

    assert limits.read_bounded(stream, max_bytes=5, resource="test response") == b"12345"
    assert max(stream.requested) <= 6
    assert sum(stream.returned) == 5


def test_bounded_reader_rejects_limit_plus_one_without_reading_more() -> None:
    stream = _ObservedStream(b"1234567")

    with pytest.raises(ValueError, match="exceeds.*5"):
        limits.read_bounded(stream, max_bytes=5, resource="test response")

    assert sum(stream.requested) == 6


@pytest.mark.parametrize("size", [True, 0, -1, limits.MAX_COMPRESSED_ARTIFACT_BYTES + 1])
def test_provider_size_must_be_positive_and_within_compressed_limit(size: int) -> None:
    with pytest.raises(ValueError, match="provider.*size"):
        limits.validate_provider_size(size, resource="receipt artifact")


def test_provider_size_accepts_exact_compressed_limit() -> None:
    limits.validate_provider_size(
        limits.MAX_COMPRESSED_ARTIFACT_BYTES,
        resource="receipt artifact",
    )


def test_zip_accepts_exact_member_and_aggregate_boundaries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(limits, "MAX_ZIP_MEMBER_BYTES", 5)
    monkeypatch.setattr(limits, "MAX_ZIP_TOTAL_BYTES", 10)
    monkeypatch.setattr(limits, "MAX_ZIP_COMPRESSION_RATIO", 100)
    raw = _archive([("a.txt", b"12345"), ("b.txt", b"67890")])

    members = limits.read_bounded_zip(raw, resource="receipt archive")

    assert {member.filename: member.content for member in members} == {
        "a.txt": b"12345",
        "b.txt": b"67890",
    }


def test_zip_rejects_member_limit_plus_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(limits, "MAX_ZIP_MEMBER_BYTES", 5)
    monkeypatch.setattr(limits, "MAX_ZIP_TOTAL_BYTES", 10)

    with pytest.raises(ValueError, match="member.*size"):
        limits.read_bounded_zip(_archive([("a.txt", b"123456")]), resource="receipt archive")


def test_zip_rejects_aggregate_limit_plus_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(limits, "MAX_ZIP_MEMBER_BYTES", 6)
    monkeypatch.setattr(limits, "MAX_ZIP_TOTAL_BYTES", 10)

    with pytest.raises(ValueError, match="aggregate"):
        limits.read_bounded_zip(
            _archive([("a.txt", b"12345"), ("b.txt", b"123456")]),
            resource="receipt archive",
        )


def test_zip_rejects_member_count_limit_plus_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(limits, "MAX_ZIP_MEMBERS", 2)

    with pytest.raises(ValueError, match="member count"):
        limits.read_bounded_zip(
            _archive([("a", b"1"), ("b", b"2"), ("c", b"3")]),
            resource="receipt archive",
        )


def test_zip_rejects_declared_compression_ratio_above_100_before_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _archive([("amplified.txt", b"0" * 10_000)])
    opened = False

    def unexpected_open(*_args: object, **_kwargs: object) -> object:
        nonlocal opened
        opened = True
        raise AssertionError("over-ratio member must fail during preflight")

    monkeypatch.setattr(zipfile.ZipFile, "open", unexpected_open)

    with pytest.raises(ValueError, match="compression ratio"):
        limits.read_bounded_zip(raw, resource="receipt archive")

    assert opened is False


def test_zip_compression_ratio_accepts_exact_limit_and_rejects_limit_plus_one() -> None:
    exact = zipfile.ZipInfo("exact.txt")
    exact.file_size = 100
    exact.compress_size = 1
    limits._preflight_members([exact], resource="receipt archive")

    oversized = zipfile.ZipInfo("oversized.txt")
    oversized.file_size = 101
    oversized.compress_size = 1
    with pytest.raises(ValueError, match="compression ratio"):
        limits._preflight_members([oversized], resource="receipt archive")


@pytest.mark.parametrize(
    "unsafe",
    ["duplicate", "parent", "absolute", "backslash", "symlink", "fifo", "encrypted"],
)
def test_zip_rejects_ambiguous_or_unsafe_members(unsafe: str) -> None:
    if unsafe == "duplicate":
        members: list[tuple[str | zipfile.ZipInfo, bytes]] = [("a.txt", b"a"), ("a.txt", b"b")]
    elif unsafe == "parent":
        members = [("../a.txt", b"a")]
    elif unsafe == "absolute":
        members = [("/a.txt", b"a")]
    elif unsafe == "backslash":
        members = [(r"..\a.txt", b"a")]
    else:
        info = zipfile.ZipInfo("a.txt")
        info.create_system = 3
        if unsafe == "symlink":
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
        elif unsafe == "fifo":
            info.external_attr = (stat.S_IFIFO | 0o644) << 16
        else:
            raw = bytearray(_archive([("a.txt", b"a")]))
            central_directory = raw.index(b"PK\x01\x02")
            flags_offset = central_directory + 8
            flags = int.from_bytes(raw[flags_offset : flags_offset + 2], "little") | 1
            raw[flags_offset : flags_offset + 2] = flags.to_bytes(2, "little")
            with pytest.raises(ValueError, match="encrypted"):
                limits.read_bounded_zip(bytes(raw), resource="receipt archive")
            return
        members = [(info, b"a")]

    with pytest.raises(ValueError, match="duplicate|unsafe|symlink|encrypted"):
        limits.read_bounded_zip(_archive(members), resource="receipt archive")


class _GateAdapter:
    def __init__(self, *, artifact_size: int, raw: bytes) -> None:
        self.artifact_size = artifact_size
        self.raw = raw
        self.downloads = 0

    def list_check_runs(self, _repository: str, _commit_sha: str) -> list[dict[str, object]]:
        return [
            {
                "id": 42,
                "name": "Agent PR receipt",
                "head_sha": INTEGRATION_SHA,
                "status": "completed",
                "conclusion": "success",
                "details_url": f"https://github.com/{REPOSITORY}/runs/42",
                "external_id": "agent-pr-merge-closure:1:1",
                "app": {"id": 15368, "slug": "github-actions"},
            }
        ]

    def get_workflow_run(self, _repository: str, _run_id: int) -> dict[str, object]:
        return {"head_sha": "a" * 40}

    def list_run_artifacts(self, _repository: str, _run_id: int) -> list[dict[str, object]]:
        return []

    def download_artifact(self, _url: str) -> bytes:
        self.downloads += 1
        return self.raw


def _gate_artifact(size: int) -> dict[str, object]:
    return {
        "id": 201,
        "digest": f"sha256:{'0' * 64}",
        "size_in_bytes": size,
        "archive_download_url": "https://api.github.test/artifacts/201/zip",
    }


def test_merge_gate_rejects_oversized_provider_metadata_before_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _GateAdapter(
        artifact_size=gate.resource_limits.MAX_COMPRESSED_ARTIFACT_BYTES + 1,
        raw=b"",
    )
    monkeypatch.setattr(gate, "_validate_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gate, "_select_artifact", lambda *_args, **_kwargs: _gate_artifact(adapter.artifact_size))

    with pytest.raises(ValueError, match="provider size.*exceeds"):
        gate.verify_merge_receipt(
            repository=REPOSITORY,
            integration_commit_sha=INTEGRATION_SHA,
            adapter=adapter,
        )

    assert adapter.downloads == 0


def test_merge_gate_rejects_oversized_download_before_digest_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gate.resource_limits, "MAX_COMPRESSED_ARTIFACT_BYTES", 5)
    adapter = _GateAdapter(artifact_size=5, raw=b"123456")
    monkeypatch.setattr(gate, "_validate_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gate, "_select_artifact", lambda *_args, **_kwargs: _gate_artifact(5))

    with pytest.raises(ValueError, match="download exceeds"):
        gate.verify_merge_receipt(
            repository=REPOSITORY,
            integration_commit_sha=INTEGRATION_SHA,
            adapter=adapter,
        )

    assert adapter.downloads == 1


def test_merge_gate_adapter_does_not_retry_terminal_resource_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def oversized(*_args: object, **_kwargs: object) -> bytes:
        nonlocal calls
        calls += 1
        raise ValueError("artifact exceeds byte limit")

    sleeps: list[float] = []
    monkeypatch.setattr(gate.github_api, "github_bytes", oversized)
    adapter = gate.GitHubMergeReceiptAdapter("token", attempts=3, sleeper=sleeps.append)

    with pytest.raises(ValueError, match="byte limit"):
        adapter.download_artifact("https://api.github.test/artifacts/201/zip")

    assert calls == 1
    assert sleeps == []
