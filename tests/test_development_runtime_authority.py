from __future__ import annotations

import base64
import hashlib
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from dpone.adapters.development_runtime_authority import (
    DevelopmentRuntimeAuthorityAdapterError,
    load_development_runtime_authority,
)
from dpone.commands.airflow_runtime_delivery_cmd import (
    cmd_airflow_runtime_init_fetch,
    cmd_airflow_runtime_pack_exec,
)
from dpone.contracts.dbt_contract_validation import artifact_json_bytes
from dpone.contracts.development_delivery_authority import (
    DEVELOPMENT_RELEASE_SCHEMA,
    DevelopmentAuthorityReceipt,
    DevelopmentExecutionSubject,
)
from dpone.ports.development_runtime_authority import (
    DevelopmentRuntimeAuthorization,
)
from dpone.readiness.airflow_runtime_init_fetch import (
    PLAN_B64_ENV,
    PLAN_SHA256_ENV,
    RUNTIME_AUTHORITY_PATH_ENV,
    AirflowRuntimeInitFetchService,
)
from dpone.readiness.development_runtime_authorization import (
    authorize_development_runtime,
    require_fetched_development_authority,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_execution import RuntimeExecutionSelection
from dpone.runtime.runtime_init_fetch_payload import ImmutableRuntimeAuthorityPayload
from dpone.runtime.runtime_init_fetch_plan import (
    canonical_runtime_init_fetch_plan_bytes,
    runtime_init_fetch_plan_sha256,
)
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.runtime_init_fetch_service import RuntimeInitFetchExecutor
from dpone.runtime.verified_pack_launcher import VerifiedPackLauncher
from tests.test_airflow_runtime_init_fetch_contract import _plan

_NOW = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)


class _Authority:
    def __init__(self, authorization: DevelopmentRuntimeAuthorization) -> None:
        self.authorization = authorization
        self.requests = []

    def authorize(self, request):
        self.requests.append(request)
        return self.authorization


class _EntryPoint:
    def __init__(self, loaded) -> None:
        self._loaded = loaded

    def load(self):
        return self._loaded


class _RegistryFactory:
    def __init__(self) -> None:
        self.called = False

    def build(self, _configuration):
        self.called = True
        raise AssertionError("registry construction must follow authorization")


class _FakeReceipt:
    environment = "dev"

    def require_current(self, **_kwargs):
        raise AssertionError("malformed receipt methods must not be called")

    def allows_execution(self, _subject):
        raise AssertionError("malformed receipt methods must not be called")


def _receipt(*, subjects: tuple[DevelopmentExecutionSubject, ...]) -> DevelopmentAuthorityReceipt:
    return DevelopmentAuthorityReceipt(
        policy_sha256="sha256:" + "1" * 64,
        grant_sha256="sha256:" + "2" * 64,
        signature_subject_sha256="sha256:" + "3" * 64,
        environment="dev",
        source_repository_sha256="sha256:" + "4" * 64,
        source_commit="5" * 40,
        not_before=(_NOW - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        expires_at=(_NOW + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        revocation_epoch=7,
        max_workloads=2,
        max_source_bytes=1_000_000,
        execution_subjects=subjects,
    )


def _authorization(receipt: DevelopmentAuthorityReceipt) -> DevelopmentRuntimeAuthorization:
    return DevelopmentRuntimeAuthorization(
        authority=receipt,
        checked_at=_NOW,
        current_revocation_epoch=7,
    )


def _development_plan():
    return replace(
        _plan(),
        execution=RuntimeExecutionSelection(
            kind="runtime",
            selector="orders",
            scope="workload",
            hook_execution="externalized",
        ),
        development_authority_required=True,
    )


def test_ordinary_plan_does_not_call_runtime_authority() -> None:
    verifier = _Authority(_authorization(_receipt(subjects=())))

    assert authorize_development_runtime(_plan(), authority=verifier) is None
    assert verifier.requests == []


def test_development_plan_authorizes_exact_runtime_subject() -> None:
    plan = _development_plan()
    receipt = _receipt(subjects=(DevelopmentExecutionSubject("orders", "runtime"),))
    verifier = _Authority(_authorization(receipt))

    result = authorize_development_runtime(plan, authority=verifier)

    assert result == _authorization(receipt)
    assert verifier.requests[0].release_id == plan.release_id
    assert verifier.requests[0].deployment_id == plan.deployment_id
    assert verifier.requests[0].runtime_image_digest == plan.runtime_image_digest


def test_development_plan_fails_closed_with_redacted_error() -> None:
    plan = _development_plan()
    verifier = _Authority(_authorization(_receipt(subjects=())))

    with pytest.raises(InitFetchError) as exc_info:
        authorize_development_runtime(plan, authority=verifier)

    assert exc_info.value.code == "DPONE_DEVELOPMENT_RUNTIME_AUTHORITY_REQUIRED"
    assert str(exc_info.value) == "development runtime requires current external authority"


def test_both_runtime_processes_deny_before_registry_or_ready_access(tmp_path) -> None:
    plan = _development_plan()
    payload = canonical_runtime_init_fetch_plan_bytes(plan)
    environment = {
        PLAN_B64_ENV: base64.b64encode(payload).decode("ascii"),
        PLAN_SHA256_ENV: runtime_init_fetch_plan_sha256(plan),
    }
    verifier = _Authority(_authorization(_receipt(subjects=())))
    registry_factory = _RegistryFactory()
    service = AirflowRuntimeInitFetchService(
        development_runtime_authority=verifier,
        registry_factory=registry_factory,
        artifact_root=tmp_path / "missing-artifacts",
        worktree_root=tmp_path / "missing-worktree",
    )

    for operation in (service.init_fetch, service.prepare_pack_exec):
        with pytest.raises(InitFetchError) as exc_info:
            operation(environment)
        assert exc_info.value.code == "DPONE_DEVELOPMENT_RUNTIME_AUTHORITY_REQUIRED"

    assert len(verifier.requests) == 2
    assert registry_factory.called is False
    assert not (tmp_path / "missing-artifacts").exists()


def test_immutable_payload_is_materialized_and_reverified_before_each_authority_call(tmp_path) -> None:
    authority_path = tmp_path / "runtime-authority" / "authority"
    authority_path.parent.mkdir()
    raw = b'{"mode":"synthetic"}\n'
    immutable = ImmutableRuntimeAuthorityPayload.from_bytes(
        raw,
        expected_sha256="sha256:" + hashlib.sha256(raw).hexdigest(),
    )
    plan = replace(_development_plan(), runtime_authority=immutable)
    payload = canonical_runtime_init_fetch_plan_bytes(plan)
    environment = {
        PLAN_B64_ENV: base64.b64encode(payload).decode("ascii"),
        PLAN_SHA256_ENV: runtime_init_fetch_plan_sha256(plan),
        RUNTIME_AUTHORITY_PATH_ENV: str(authority_path),
    }
    verifier = _Authority(_authorization(_receipt(subjects=())))
    service = AirflowRuntimeInitFetchService(
        development_runtime_authority=verifier,
        registry_factory=_RegistryFactory(),
        runtime_authority_path=authority_path,
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    with pytest.raises(InitFetchError, match="development runtime requires current external authority"):
        service.init_fetch(environment)
    assert authority_path.read_bytes() == raw
    assert len(verifier.requests) == 1

    authority_path.chmod(0o600)
    authority_path.write_bytes(b"tampered")
    with pytest.raises(InitFetchError) as exc_info:
        service.prepare_pack_exec(environment)
    assert exc_info.value.code == "DPONE_RUNTIME_AUTHORITY_PAYLOAD_INVALID"
    assert len(verifier.requests) == 1


@pytest.mark.parametrize(
    "execution",
    (
        RuntimeExecutionSelection(
            kind="runtime",
            selector="orders",
            scope="workload",
            hook_execution="externalized",
        ),
        RuntimeExecutionSelection(
            kind="pre_hook",
            selector="orders",
            scope="process",
            process_selector="orders",
            hook_name="prepare_orders",
            hook_execution="externalized",
        ),
    ),
)
def test_malformed_inner_receipt_is_denied_before_registry(execution, tmp_path) -> None:
    plan = replace(_plan(), execution=execution, development_authority_required=True)
    payload = canonical_runtime_init_fetch_plan_bytes(plan)
    environment = {
        PLAN_B64_ENV: base64.b64encode(payload).decode("ascii"),
        PLAN_SHA256_ENV: runtime_init_fetch_plan_sha256(plan),
    }
    registry_factory = _RegistryFactory()
    malformed = DevelopmentRuntimeAuthorization(
        authority=_FakeReceipt(),
        checked_at=_NOW,
        current_revocation_epoch=7,
    )
    service = AirflowRuntimeInitFetchService(
        development_runtime_authority=_Authority(malformed),
        registry_factory=registry_factory,
        artifact_root=tmp_path / "missing-artifacts",
        worktree_root=tmp_path / "missing-worktree",
    )

    with pytest.raises(InitFetchError) as exc_info:
        service.init_fetch(environment)

    assert exc_info.value.code == "DPONE_DEVELOPMENT_RUNTIME_AUTHORITY_REQUIRED"
    assert registry_factory.called is False
    assert not (tmp_path / "missing-artifacts").exists()


@pytest.mark.parametrize(
    "command",
    (cmd_airflow_runtime_init_fetch, cmd_airflow_runtime_pack_exec),
)
def test_cli_redacts_image_adapter_discovery_failures(command, monkeypatch, caplog) -> None:
    plan = _development_plan()
    payload = canonical_runtime_init_fetch_plan_bytes(plan)
    monkeypatch.setenv(PLAN_B64_ENV, base64.b64encode(payload).decode("ascii"))
    monkeypatch.setenv(PLAN_SHA256_ENV, runtime_init_fetch_plan_sha256(plan))

    def fail_to_load():
        raise DevelopmentRuntimeAuthorityAdapterError("private adapter detail")

    monkeypatch.setattr(
        "dpone.readiness.airflow_runtime_init_fetch.load_development_runtime_authority",
        fail_to_load,
    )

    with caplog.at_level(logging.ERROR):
        exit_code = command(object(), ctx=object(), logger=logging.getLogger(__name__))

    assert exit_code == 4
    assert "DPONE_DEVELOPMENT_RUNTIME_AUTHORITY_REQUIRED" in caplog.text
    assert "development runtime requires current external authority" in caplog.text
    assert "private adapter detail" not in caplog.text


def test_low_level_runtime_components_cannot_bypass_development_authority(tmp_path) -> None:
    plan = _development_plan()
    executor = RuntimeInitFetchExecutor(
        registry=object(),
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )
    launcher = VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    )

    for operation in (
        lambda: executor.execute(plan, plan_sha256=runtime_init_fetch_plan_sha256(plan)),
        lambda: launcher.prepare(plan, plan_sha256=runtime_init_fetch_plan_sha256(plan)),
    ):
        with pytest.raises(InitFetchError) as exc_info:
            operation()
        assert exc_info.value.code == "DPONE_DEVELOPMENT_RUNTIME_AUTHORITY_REQUIRED"

    assert not (tmp_path / "artifacts").exists()


def test_fetched_release_must_match_current_authority() -> None:
    plan = _development_plan()
    receipt = _receipt(subjects=(DevelopmentExecutionSubject("orders", "runtime"),))
    authorization = _authorization(receipt)
    release = {
        "schema": DEVELOPMENT_RELEASE_SCHEMA,
        "development_authority": receipt.release_projection(),
    }

    require_fetched_development_authority(
        artifact_json_bytes(release),
        plan=plan,
        authorization=authorization,
    )
    release["development_authority"]["grant_sha256"] = "sha256:" + "9" * 64
    with pytest.raises(InitFetchError) as exc_info:
        require_fetched_development_authority(
            artifact_json_bytes(release),
            plan=plan,
            authorization=authorization,
        )
    assert exc_info.value.code == "DPONE_DEVELOPMENT_RUNTIME_AUTHORITY_REQUIRED"


def test_pre_hook_requires_its_exact_hook_subject() -> None:
    plan = replace(
        _plan(),
        execution=RuntimeExecutionSelection(
            kind="pre_hook",
            selector="orders",
            scope="process",
            process_selector="orders",
            hook_name="prepare_orders",
            hook_execution="externalized",
        ),
        development_authority_required=True,
    )
    receipt = _receipt(
        subjects=(
            DevelopmentExecutionSubject(
                "orders",
                "pre_hook",
                "prepare_orders",
            ),
        )
    )
    authorization = _authorization(receipt)
    release = {
        "schema": DEVELOPMENT_RELEASE_SCHEMA,
        "development_authority": receipt.release_projection(),
    }

    assert (
        authorize_development_runtime(
            plan,
            authority=_Authority(authorization),
        )
        == authorization
    )
    require_fetched_development_authority(
        artifact_json_bytes(release),
        plan=plan,
        authorization=authorization,
    )


def test_plan_v4_round_trip_is_canonical_and_closed() -> None:
    plan = _development_plan()
    payload = canonical_runtime_init_fetch_plan_bytes(plan)

    decoded, digest = decode_runtime_init_fetch_plan(
        base64.b64encode(payload).decode("ascii"),
        runtime_init_fetch_plan_sha256(plan),
    )

    assert decoded.development_authority_required is True
    assert decoded.to_dict()["schema"] == "dpone.airflow-runtime-init-fetch-plan.v4"
    assert digest == runtime_init_fetch_plan_sha256(plan)


@pytest.mark.parametrize("count", (0, 2))
def test_image_adapter_loader_requires_exactly_one_entry_point(count: int) -> None:
    with pytest.raises(DevelopmentRuntimeAuthorityAdapterError):
        load_development_runtime_authority(discover=lambda: tuple(_EntryPoint(lambda: object()) for _ in range(count)))


def test_image_adapter_loader_constructs_authority() -> None:
    adapter = _Authority(_authorization(_receipt(subjects=(DevelopmentExecutionSubject("orders", "runtime"),))))

    assert load_development_runtime_authority(discover=lambda: (_EntryPoint(lambda: adapter),)) is adapter
