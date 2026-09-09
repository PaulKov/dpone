from __future__ import annotations

import json
import sys
import types
from collections.abc import Sequence
from pathlib import Path

import pytest

from dpone.cli import main as cli_main
from dpone.readiness.airflow_artifact_delivery import ArtifactRegistryOptions
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.object_storage_access_models import ObjectStorageConnectionRef
from dpone.runtime.object_storage_connection_resolver import (
    ObjectStorageConnectionResolver,
)
from dpone.storage.models import ObjectStorageUri
from tests.support.airflow_artifact_projection import publish_test_projection


def test_help_exposes_only_bounded_logical_credential_references(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(["airflow", "cache-materialize", "--help"], capsys)

    assert code == 0
    assert stderr == ""
    assert "--connection-id" in stdout
    assert "--connection-type" in stdout
    assert "params" not in stdout
    assert "--vault-path" not in stdout
    assert "--vault-mount-point" not in stdout


def test_cli_injects_logical_connection_into_existing_registry_composition(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    def materialize(**kwargs: object) -> SelfServiceResult:
        captured.update(kwargs)
        return SelfServiceResult(passed=True, details={"status": "no_op"}, exit_code=0)

    monkeypatch.setattr(
        "dpone.commands.airflow_artifact_delivery_cmd.materialize_command_result",
        materialize,
    )

    code, stdout, stderr = _run_cli(_logical_connection_args(), capsys)

    assert code == 0
    assert stderr == ""
    assert json.loads(stdout)["status"] == "no_op"
    options = captured["registry_options"]
    assert isinstance(options, ArtifactRegistryOptions)
    assert options.connection_type == "airflow"
    assert options.connection_id == "s3_dpone_artifacts_reader"


def test_cli_rejects_secret_shaped_connection_id_without_echo(
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = '{"password":"do-not-print"}'
    args = _logical_connection_args()
    args[args.index("s3_dpone_artifacts_reader")] = secret

    code, stdout, stderr = _run_cli(args, capsys)

    assert code == 2
    assert stderr == ""
    assert "do-not-print" not in stdout
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_ARTIFACT_DELIVERY_INPUT_INVALID"


def test_cli_rejects_multiple_access_modes_before_materialization(
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = _logical_connection_args()
    args.extend(["--identity-mode", "workload_identity"])

    code, stdout, stderr = _run_cli(args, capsys)

    assert code == 2
    assert stdout == ""
    assert "not allowed with argument" in stderr


def test_logical_connection_materializes_exact_projection_without_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    client, release_id, deployment_id = publish_test_projection(tmp_path)
    captured: dict[str, object] = {}

    class FakeResolver:
        def build_client(self, *, ref: object, uri: object) -> object:
            captured["ref"] = ref
            captured["uri"] = uri
            return client

    monkeypatch.setattr(
        "dpone.readiness.airflow_artifact_delivery.ObjectStorageConnectionResolver",
        FakeResolver,
    )
    target = tmp_path / "target-cache"
    args = _logical_connection_args()
    args[args.index("sha256:" + "a" * 64)] = release_id
    args[args.index("sha256:" + "b" * 64)] = deployment_id
    args[args.index("dpone-dev-artifacts")] = "local-test"
    args[args.index("s3://example-data-bucket/dpone-artifacts/prod/example-workloads")] = "s3://dpone-artifacts/airflow"
    args.extend(["--cache-root", str(target)])

    first_code, first_stdout, first_stderr = _run_cli(args, capsys)
    second_code, second_stdout, second_stderr = _run_cli(args, capsys)

    assert (first_code, second_code) == (0, 0)
    assert first_stderr == second_stderr == ""
    assert json.loads(first_stdout)["status"] == "materialized"
    assert json.loads(second_stdout)["status"] == "no_op"
    assert not (target / "current").exists()
    ref = captured["ref"]
    assert isinstance(ref, ObjectStorageConnectionRef)
    assert ref.connection_type == "airflow"
    assert ref.connection_id == "s3_dpone_artifacts_reader"


@pytest.mark.parametrize(
    "credentials",
    (
        CredentialsConfig(),
        CredentialsConfig(username="access-only"),
        CredentialsConfig(password="secret-only"),
    ),
)
def test_shared_publish_and_materialize_s3_resolver_rejects_incomplete_credentials_before_io(
    credentials: CredentialsConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setitem(
        sys.modules,
        "boto3",
        types.SimpleNamespace(client=lambda service, **kwargs: calls.append((service, kwargs))),
    )
    resolver = ObjectStorageConnectionResolver(credentials_manager=_StaticCredentialsManager(credentials))

    with pytest.raises(ValueError, match="requires explicit"):
        resolver.build_client(
            ref=ObjectStorageConnectionRef(
                connection_type="env",
                connection_id="s3_reader",
            ),
            uri=ObjectStorageUri.parse("s3://bucket/releases"),
        )

    assert calls == []


def test_logical_resolver_failure_is_redacted_from_cli_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "secret_key=must-not-leak"

    class FailingResolver:
        def build_client(self, **_: object) -> object:
            raise RuntimeError(secret)

    monkeypatch.setattr(
        "dpone.readiness.airflow_artifact_delivery.ObjectStorageConnectionResolver",
        FailingResolver,
    )

    code, stdout, stderr = _run_cli(_logical_connection_args(), capsys)

    assert code == 3
    assert stderr == ""
    assert secret not in stdout
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE"


def _logical_connection_args() -> list[str]:
    return [
        "airflow",
        "cache-materialize",
        "--release-id",
        "sha256:" + "a" * 64,
        "--deployment-id",
        "sha256:" + "b" * 64,
        "--environment",
        "dev",
        "--artifact-registry-ref",
        "dpone-dev-artifacts",
        "--registry-uri",
        "s3://example-data-bucket/dpone-artifacts/prod/example-workloads",
        "--connection-type",
        "airflow",
        "--connection-id",
        "s3_dpone_artifacts_reader",
        "--format",
        "json",
    ]


def _run_cli(
    args: Sequence[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    try:
        cli_main.main(list(args))
        code = 0
    except SystemExit as exc:
        code = int(exc.code or 0)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


class _StaticCredentialsManager:
    def __init__(self, credentials: CredentialsConfig) -> None:
        self._credentials = credentials

    def get_credentials(self, *_: object, **__: object) -> CredentialsConfig:
        return self._credentials
