"""The opt-in verifier has a closed identity separate from legacy BCP work."""

from dataclasses import FrozenInstanceError, replace

import pytest

from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.contracts.mssql_native_verification import NativeVerificationBackend, NativeVerificationIdentityV2


def _plan() -> NativeChunkPlan:
    return NativeChunkPlan("run", "target", "query", "window", "schema", "wire")


def _identity(**changes: object) -> NativeVerificationIdentityV2:
    values = dict(
        plan=_plan(),
        import_backend="bcp",
        verification_backend=NativeVerificationBackend.TARGET_LOCAL,
        companion_protocol_sha256="a" * 64,
        companion_package_sha256="b" * 64,
        capability_layout_sha256="c" * 64,
        digest_algorithm_id="mssql-native-sha256-sum-v1",
        timeout_policy_sha256="d" * 64,
    )
    values.update(changes)
    return NativeVerificationIdentityV2(**values)


def test_omitted_selector_keeps_legacy_verifier_and_identity_version():
    assert NativeVerificationBackend.parse(None) is NativeVerificationBackend.PYTHON_READBACK
    assert NativeVerificationBackend.PYTHON_READBACK.identity_version == 1
    assert NativeVerificationBackend.parse("target_local").identity_version == 2


@pytest.mark.parametrize("value", ["", "python", "TARGET_LOCAL", "unknown", 1, True])
def test_unknown_verification_selector_fails_closed(value):
    with pytest.raises(ValueError, match="verification_backend"):
        NativeVerificationBackend.parse(value)


def test_v2_identity_is_immutable_and_binds_all_versioned_inputs():
    identity = _identity()
    assert identity.document() == {
        "run_id": "run",
        "target_id": "target",
        "source_query_id": "query",
        "window_fingerprint": "window",
        "schema_fingerprint": "schema",
        "wire_fingerprint": "wire",
        "import_backend": "bcp",
        "verification_backend": "target_local",
        "writer_proof_capability": "bcp-supervised-stage-barrier-v1",
        "companion_protocol_sha256": "a" * 64,
        "companion_package_sha256": "b" * 64,
        "capability_layout_sha256": "c" * 64,
        "digest_algorithm_id": "mssql-native-sha256-sum-v1",
        "timeout_policy_sha256": "d" * 64,
    }
    with pytest.raises(FrozenInstanceError):
        identity.import_backend = "mssql_sqlclient"
    assert len(identity.invocation_key) == 64
    assert identity.invocation_key == _identity().invocation_key
    assert replace(identity, timeout_policy_sha256="e" * 64).invocation_key != identity.invocation_key
    assert (
        replace(
            identity, import_backend="mssql_sqlclient", writer_proof_capability="sqlclient-session-applock-v1"
        ).invocation_key
        != identity.invocation_key
    )


def test_v2_identity_rejects_mismatched_backend_and_proof_capability():
    with pytest.raises(ValueError, match="identity"):
        _identity(import_backend="bcp", writer_proof_capability="sqlclient-session-applock-v1")
    with pytest.raises(ValueError, match="identity"):
        _identity(import_backend="mssql_sqlclient", writer_proof_capability="bcp-supervised-stage-barrier-v1")


@pytest.mark.parametrize(
    "changes",
    [
        {"verification_backend": NativeVerificationBackend.PYTHON_READBACK},
        {"import_backend": "unknown"},
        {"companion_package_sha256": "A" * 64},
        {"capability_layout_sha256": "short"},
        {"digest_algorithm_id": ""},
        {"plan": replace(_plan(), target_id="")},
    ],
)
def test_v2_identity_rejects_legacy_or_malformed_bindings(changes):
    with pytest.raises(ValueError, match="identity"):
        _identity(**changes)
