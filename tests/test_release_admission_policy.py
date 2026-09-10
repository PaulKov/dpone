"""Characterize public admission failure order before separating pure policies."""

import pytest

from dpone.gitops.release_set_validation import validate_release_set
from dpone.readiness.dbt_publish_release_materializer import DbtReleaseMaterializationError, DbtReleaseMaterializer


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("dpone.release-set.v99", "DPONE_RELEASE_SCHEMA_INVALID"),
        ("dpone.release-set.v1", "DPONE_DEPLOYMENT_DIGEST_INVALID"),
        ("dpone.release-set.v2", "DPONE_DEPLOYMENT_DIGEST_INVALID"),
        ("dpone.release-set.v3", "DPONE_DEPLOYMENT_DIGEST_INVALID"),
    ],
)
def test_generic_admission_preserves_first_failure_for_simultaneous_faults(kind, expected):
    release = {"schema": kind, "producer": {}, "artifacts": {"workload_packs": [{"sha256": "invalid"}]}}
    result = validate_release_set(release)
    assert result.failure is not None and result.failure.code == expected
    assert result.dbt_runtime_wire_contract is None


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"{", "compiled dbt release-set is invalid JSON"),
        (b'{"schema":"dpone.release-set.v1","producer":{}}', "compiled dbt release must use dpone.release-set.v2"),
    ],
)
def test_native_admission_rejects_before_creating_cache(tmp_path, payload, expected):
    root = tmp_path / "source"
    root.mkdir()
    (root / "release-set.json").write_bytes(payload)
    cache = tmp_path / "cache"
    with pytest.raises(DbtReleaseMaterializationError) as error:
        DbtReleaseMaterializer().materialize(compiled_root=root, cache_root=cache)
    assert str(error.value) == expected
    assert error.value.code == "DPONE_DBT_PROJECT_BUNDLE_INVALID"
    assert not cache.exists()
    assert (root / "release-set.json").read_bytes() == payload


def test_generic_schema_failure_precedes_native_authority():
    result = validate_release_set({"schema": "dpone.release-set.v2", "producer": {}})
    assert result.failure is not None
    assert result.failure.message == "release-set violates its public schema"


def test_generic_compatibility_exports_retain_canonical_class_identity():
    from dpone.contracts import release_set_authority as canonical
    from dpone.gitops import release_set_validation as compatibility

    assert compatibility.ReleaseSetValidation is canonical.ReleaseSetValidation
    assert compatibility.ReleaseSetValidationFailure is canonical.ReleaseSetValidationFailure
    assert compatibility.release_activation_failure is canonical.release_activation_failure
    result = compatibility.validate_release_set({"schema": "unknown"})
    assert isinstance(result, canonical.ReleaseSetValidation)
    assert isinstance(result.failure, canonical.ReleaseSetValidationFailure)
