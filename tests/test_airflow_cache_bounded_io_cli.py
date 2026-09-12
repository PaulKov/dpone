from __future__ import annotations

import json
import sys
from contextlib import nullcontext
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from dpone_airflow_pack import cache_sync_evidence, cli_cache_status, cli_sync
from dpone_airflow_pack.artifact_store import ArtifactReader, ArtifactStoreError
from dpone_airflow_pack.artifact_uri_redaction import redact_artifact_uri
from dpone_airflow_pack.cache_layout import LEGACY_PACK_INDEX_LAYOUT
from dpone_airflow_pack.cache_sync import AirflowPackSyncOptions


class _TrackingBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        if size < 0:
            raise AssertionError("artifact body was read without a bound")
        return self.payload[:size]

    def __enter__(self) -> _TrackingBody:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def test_local_reader_uses_one_sentinel_byte_during_read(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_bytes(b"1234567")

    with pytest.raises(ArtifactStoreError, match="artifact exceeds max_bytes"):
        ArtifactReader().read_bytes(artifact.as_uri(), max_bytes=5)


def test_s3_reader_uses_one_sentinel_byte_during_read(monkeypatch: pytest.MonkeyPatch) -> None:
    body = _TrackingBody(b"1234567")
    _install_fake_s3(monkeypatch, body=body, content_length=None)

    with pytest.raises(ArtifactStoreError, match="artifact exceeds max_bytes"):
        ArtifactReader(reader_connection_id="artifact_reader").read_bytes(
            "s3://bucket/path/artifact.json",
            max_bytes=5,
        )

    assert body.read_sizes == [6]


def test_s3_reader_rejects_declared_oversize_before_body_read(monkeypatch: pytest.MonkeyPatch) -> None:
    body = _TrackingBody(b"not-read")
    _install_fake_s3(monkeypatch, body=body, content_length=7)

    with pytest.raises(ArtifactStoreError, match="artifact exceeds max_bytes"):
        ArtifactReader().read_bytes("s3://bucket/path/artifact.json", max_bytes=5)

    assert body.read_sizes == []


def test_reader_and_sync_options_preserve_explicit_python_none_semantics(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_bytes(b"unbounded-python-api-payload")

    assert ArtifactReader().read_bytes(artifact.as_uri(), max_bytes=None) == artifact.read_bytes()
    options = AirflowPackSyncOptions(
        index_uri=artifact.as_uri(),
        cache_dir=tmp_path / "cache",
        max_total_bytes=None,
        max_pack_bytes=None,
        max_index_bytes=None,
    )
    assert (options.max_total_bytes, options.max_pack_bytes, options.max_index_bytes) == (None, None, None)


def test_uri_redaction_removes_userinfo_and_sensitive_query_values() -> None:
    uri = (
        "https://vqzxv:zxvqzxv-zxvqzxvq@storage.example/release/pack.json"
        "?region=eu&token=xvqzxvq-xvqzx&X-Amz-Signature=qzxvqzx-qzxvqzxvq"
    )

    redacted = redact_artifact_uri(uri)

    assert "vqzxv" not in redacted
    assert "zxvqzxv-zxvqzxvq" not in redacted
    assert "xvqzxvq-xvqzx" not in redacted
    assert "qzxvqzx-qzxvqzxvq" not in redacted
    assert "region=[REDACTED]" in redacted
    assert "%5BREDACTED%5D@storage.example" in redacted
    assert redacted.count("[REDACTED]") == 3


def test_uri_redaction_removes_bare_query_component_and_fragment() -> None:
    uri = "https://storage.example/release/pack.json?bare-secret-token#private-fragment"

    redacted = redact_artifact_uri(uri)

    assert "bare-secret-token" not in redacted
    assert "private-fragment" not in redacted
    assert redacted.endswith("?[REDACTED]#[REDACTED]")


def test_uri_redaction_fails_safe_for_malformed_authority() -> None:
    uri = "https://xvqzx:qzxvqzx-qzxvqzxv@[invalid]/pack.json?token=zxvqzxv-zxvqz"

    redacted = redact_artifact_uri(uri)

    assert "xvqzx" not in redacted
    assert "qzxvqzx-qzxvqzxv" not in redacted
    assert "zxvqzxv-zxvqz" not in redacted
    assert "%5BREDACTED%5D@[invalid]" in redacted
    assert "token=[REDACTED]" in redacted


def test_sync_evidence_redacts_nested_uris_in_stdout_file_and_airflow_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    uri = "https://alice:private-password@storage.example/latest/pack-index.json?region=eu&token=private-token"
    status_path = tmp_path / "last-sync-status.json"
    metadata_payloads = _isolate_evidence_io(monkeypatch)
    options = SimpleNamespace(
        index_uri=uri,
        cache_dir=tmp_path / "cache",
        status_path=status_path,
        airflow_variable_key="dpone_status",
    )
    receipt = SimpleNamespace(
        durable=True,
        generation="generation-1",
        commit_id="commit-1",
        sequence=1,
        index_sha256="a" * 64,
    )
    monkeypatch.setattr(
        cache_sync_evidence,
        "read_legacy_cache_authority",
        lambda _root: SimpleNamespace(receipt=receipt, is_consistent_durable=True),
    )

    evidence = cache_sync_evidence.publish_sync_evidence(
        options,
        started_at="2026-08-02T00:00:00Z",
        attempt_generation="generation-1",
        downloaded=0,
        downloaded_specs=0,
        committed=False,
        warnings=[f"retry {uri}"],
        blockers=[{"code": "remote_failed", "message": f"cannot read {uri}"}],
        cache_bytes=0,
    )
    monkeypatch.setattr(cli_sync, "sync_airflow_pack_cache", lambda _options: evidence)

    exit_code = cli_sync.main(_sync_arguments(tmp_path, index_uri=uri))

    captured = capsys.readouterr()
    serialized_channels = json.dumps(
        {
            "stdout": json.loads(captured.out),
            "status_file": json.loads(status_path.read_text(encoding="utf-8")),
            "metadata": metadata_payloads,
        },
        sort_keys=True,
    )
    assert exit_code == 1
    assert captured.err == ""
    assert "private-password" not in serialized_channels
    assert "private-token" not in serialized_channels
    assert "alice" not in serialized_channels
    assert "region=[REDACTED]" in serialized_channels
    assert "[REDACTED]" in serialized_channels


def test_sync_failure_redacts_uri_in_stderr_and_warning_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    uri = "https://alice:private-password@storage.example/index.json?token=private-token"
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    status_path = tmp_path / "warning.json"
    metadata_payloads = _isolate_evidence_io(monkeypatch)
    monkeypatch.setattr(cache_sync_evidence, "read_cache_layout", lambda _root: LEGACY_PACK_INDEX_LAYOUT)
    monkeypatch.setattr(
        cache_sync_evidence,
        "read_legacy_cache_authority",
        lambda _root: SimpleNamespace(receipt=None, is_consistent_durable=False),
    )

    def fail(_options: object) -> None:
        raise ValueError(f"remote request failed for {uri}")

    monkeypatch.setattr(cli_sync, "sync_airflow_pack_cache", fail)

    exit_code = cli_sync.main(
        [
            *_sync_arguments(tmp_path, index_uri=uri, cache_dir=cache_dir),
            "--status-path",
            str(status_path),
            "--airflow-variable-key",
            "dpone_status",
        ]
    )

    captured = capsys.readouterr()
    serialized_channels = json.dumps(
        {
            "stderr": captured.err,
            "status_file": json.loads(status_path.read_text(encoding="utf-8")),
            "metadata": metadata_payloads,
        },
        sort_keys=True,
    )
    assert exit_code == 1
    assert captured.out == ""
    assert "private-password" not in serialized_channels
    assert "private-token" not in serialized_channels
    assert "alice" not in serialized_channels
    assert "[REDACTED]" in serialized_channels


def test_sync_warning_preserves_previous_success_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_path = tmp_path / "status.json"
    status_path.write_text(
        json.dumps({"status": "success", "finished_at": "2026-08-03T01:02:03Z"}),
        encoding="utf-8",
    )
    options = SimpleNamespace(
        index_uri="s3://bucket/latest/pack-index.json",
        cache_dir=tmp_path / "cache",
        status_path=status_path,
        airflow_variable_key=None,
    )
    monkeypatch.setattr(cache_sync_evidence, "read_cache_layout", lambda _root: None)
    monkeypatch.setenv("DPONE_AIRFLOW_PACK_SYNC_COMPONENT", "dag-processor")

    with pytest.warns(RuntimeWarning, match="DPONE_AIRFLOW_PACK_EXTERNAL_WARNING_UNPUBLISHED"):
        evidence = cache_sync_evidence.write_sync_warning(
            options,
            reason="watch_sync_failed",
            message="remote unavailable",
        )

    assert evidence["component"] == "dag-processor"
    assert evidence["last_success_at"] == "2026-08-03T01:02:03Z"


@pytest.mark.parametrize("option", ["--max-total-bytes", "--max-pack-bytes", "--max-index-bytes"])
def test_empty_cli_byte_limit_fails_before_cache_root_mutation(
    option: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_dir = tmp_path / "cache"
    called = False

    def unexpected_sync(_options: object) -> dict[str, Any]:
        nonlocal called
        called = True
        return {"status": "success"}

    monkeypatch.setattr(cli_sync, "sync_airflow_pack_cache", unexpected_sync)

    with pytest.raises(SystemExit) as failed:
        cli_sync.main([*_sync_arguments(tmp_path, cache_dir=cache_dir), option, ""])

    captured = capsys.readouterr()
    assert failed.value.code == 2
    assert captured.out == ""
    assert option in captured.err
    assert "positive byte size" in captured.err
    assert called is False
    assert not cache_dir.exists()


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--keep-generations", "0"], "positive integer"),
        (["--partial-download-ttl-minutes", "0"], "positive integer"),
        (["--interval-seconds", "0"], "positive integer"),
        (["--jitter-seconds", "-1"], "non-negative integer"),
        (["--low-watermark-pct", "80", "--high-watermark-pct", "80"], "watermarks"),
    ],
)
def test_invalid_cli_policy_fails_before_cache_root_mutation(
    arguments: list[str],
    message: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_dir = tmp_path / "cache"
    monkeypatch.setattr(
        cli_sync,
        "sync_airflow_pack_cache",
        lambda _options: pytest.fail("invalid policy must not reach sync"),
    )

    with pytest.raises(SystemExit) as failed:
        cli_sync.main([*_sync_arguments(tmp_path, cache_dir=cache_dir), *arguments])

    captured = capsys.readouterr()
    assert failed.value.code == 2
    assert captured.out == ""
    assert message in captured.err
    assert not cache_dir.exists()


def test_sync_cli_help_explains_roots_units_defaults_output_and_failures(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as shown:
        cli_sync.main(["--help"])

    help_text = " ".join(capsys.readouterr().out.split())
    assert shown.value.code == 0
    assert "legacy_pack_index_v1 cache root" in help_text
    assert "exact_deployment_v1" in help_text
    assert "KiB, MiB, GiB" in help_text
    assert "512 MiB" in help_text
    assert "JSON evidence" in help_text
    assert "exit 1" in help_text
    assert "Python API" in help_text


def test_cache_status_cli_help_explains_root_layout_output_and_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as shown:
        cli_cache_status.main(["--help"])

    help_text = " ".join(capsys.readouterr().out.split())
    assert shown.value.code == 0
    assert "read-only" in help_text.lower()
    assert "DPONE_AIRFLOW_PACK_CACHE_DIR" in help_text
    assert "legacy_pack_index_v1" in help_text
    assert "exact_deployment_v1" in help_text
    assert "JSON" in help_text
    assert "exit 1" in help_text


def _install_fake_s3(
    monkeypatch: pytest.MonkeyPatch,
    *,
    body: _TrackingBody,
    content_length: int | None,
) -> None:
    class FakeObject:
        def get(self) -> dict[str, object]:
            response: dict[str, object] = {"Body": body}
            if content_length is not None:
                response["ContentLength"] = content_length
            return response

    class FakeS3Hook:
        def __init__(self, *, aws_conn_id: str | None) -> None:
            self.aws_conn_id = aws_conn_id

        def get_key(self, *, key: str, bucket_name: str) -> FakeObject:
            assert key == "path/artifact.json"
            assert bucket_name == "bucket"
            return FakeObject()

    module_names = (
        "airflow",
        "airflow.providers",
        "airflow.providers.amazon",
        "airflow.providers.amazon.aws",
        "airflow.providers.amazon.aws.hooks",
    )
    for module_name in module_names:
        module = ModuleType(module_name)
        module.__path__ = []  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, module_name, module)
    s3_module = ModuleType("airflow.providers.amazon.aws.hooks.s3")
    s3_module.S3Hook = FakeS3Hook  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, s3_module.__name__, s3_module)


def _isolate_evidence_io(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    metadata_payloads: list[dict[str, Any]] = []

    class CapturingPublisher:
        def __init__(self, _variable_key: str | None) -> None:
            pass

        def publish(self, evidence: dict[str, Any]) -> dict[str, Any]:
            payload = json.loads(json.dumps(evidence))
            metadata_payloads.append(payload)
            return payload

    monkeypatch.setattr(cache_sync_evidence, "cache_evidence_lease", lambda _root: nullcontext())
    monkeypatch.setattr(cache_sync_evidence, "cache_write_lease", lambda _root: nullcontext())
    monkeypatch.setattr(cache_sync_evidence, "AirflowVariableStatusPublisher", CapturingPublisher)
    return metadata_payloads


def _sync_arguments(
    tmp_path: Path,
    *,
    index_uri: str = "s3://bucket/latest/pack-index.json",
    cache_dir: Path | None = None,
) -> list[str]:
    return [
        "--once",
        "--index-uri",
        index_uri,
        "--cache-dir",
        str(cache_dir or tmp_path / "cache"),
    ]
