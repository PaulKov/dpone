"""Catalog projections consume genuine release/archive/member verification."""

from dataclasses import replace

import pytest

from dpone.adapters.dbt_mssql_physical_catalog_policy import MssqlPhysicalCatalogPolicyReader
from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import PlatformSelection, RegisteredLimits
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject
from dpone.manifest.confined_files import read_confined_file
from tests.support.dbt_mssql_physical_registration import registration_inputs
from tests.test_native_original_verification import _native_verifier, _real_native_projection


def test_legacy_policy_remains_valid_but_catalog_requires_explicit_limits(tmp_path, monkeypatch):
    fixture = _real_native_projection(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        reader = MssqlPhysicalCatalogPolicyReader(
            verifier=verifier,
            documents=NativeProjectDocumentReader(read_file=read_confined_file, max_policy_bytes=1024 * 1024),
        )
        with pytest.raises(ValueError, match="physical_catalog_limits"):
            reader.read(fixture.refs, registration=MssqlPhysicalRuntimeRegistration(**registration_inputs()))


def _fixture(tmp_path, monkeypatch, *, policy_change=None):
    from tests import test_dbt_native_policy_v4 as policy_module

    original_policy = policy_module.native_policy

    def policy():
        value = original_policy()
        profile = value["profiles"]["local"]
        profile["authoring_template"] = {
            "project_name": "orders",
            "invocation_target": {"database": "analytics", "schema": "base"},
            "source_relation": {"database": "example", "schema": "dbo", "name": "orders"},
        }
        profile["native_execution"]["physical_catalog_limits"] = {
            "max_catalog_rows": 100,
            "max_dependency_rows": 10,
            "max_definition_utf16_bytes": 4096,
            "max_columns": 256,
        }
        if policy_change:
            policy_change(profile)
        return value

    monkeypatch.setattr(policy_module, "native_policy", policy)
    return _real_native_projection(tmp_path, monkeypatch)


def _claim(verifier, fixture):
    import json

    original = verifier.resolve(fixture.refs)
    profile = json.loads(original.policy_document)["profiles"]["local"]
    native = profile["native_execution"]
    subject = NativePlatformOriginalSubject(original.authority, original.policy_sha256)
    from dpone.contracts.native_delivery_json import encode_native_delivery_json
    from dpone.contracts.native_originals import decode_native_original_subject

    def selection(value):
        selected_subject = (
            subject
            if value["subject"] is None
            else decode_native_original_subject(encode_native_delivery_json(value["subject"]))
        )
        return PlatformSelection(OriginalRef(**value["reference"]), selected_subject)

    fields = registration_inputs()
    pin = replace(fields["model_database"], database_name="analytics")
    fields.update(
        model_connection_ref="source-main",
        model_database=pin,
        control_database=pin,
        platform_subject=subject,
        trusted_profile=selection(native["trusted_execution"]["profile"]),
        trusted_toolchain=selection(native["trusted_execution"]["toolchain"]),
        control_authority=OriginalRef(**native["control"]["authority"]),
        control_connection_ref=native["control"]["connection_ref"],
        control_schema=native["control"]["schema"],
        capacity_authority=OriginalRef(**native["generation"]["capacity_authority"]),
        qualification_policy_id=native["trusted_execution"]["qualification_policy_id"],
        limits=RegisteredLimits(
            native["limits"]["max_metadata_bytes"],
            native["generation"]["max_generation_bytes"],
            **native["physical_catalog_limits"],
        ),
    )
    return MssqlPhysicalRuntimeRegistration(**fields)


def _reader(verifier):
    return MssqlPhysicalCatalogPolicyReader(
        verifier=verifier,
        documents=NativeProjectDocumentReader(read_file=read_confined_file, max_policy_bytes=1024 * 1024),
    )


def test_real_archive_projection_is_detached_and_exact(tmp_path, monkeypatch):
    from dataclasses import FrozenInstanceError

    from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest

    fixture = _fixture(tmp_path, monkeypatch)
    calls = []
    with _native_verifier(fixture, calls) as verifier:
        claim = _claim(verifier, fixture)
        projection = _reader(verifier).read(fixture.refs, registration=claim)
        assert projection.registration_sha256 == physical_runtime_registration_digest(claim)
        assert projection.platform_subject == claim.platform_subject
        assert projection.trusted_profile == claim.trusted_profile
        assert projection.resource_bounds == claim.trusted_profile.reference
        assert projection.profile_name == "local"
        assert projection.workflow_id == "alpha"
        assert (projection.model_database_name, projection.model_schema) == ("analytics", "base")
        assert projection.policy_member.sha256 == claim.platform_subject.platform_policy_sha256
        assert projection.project_archive_sha256.startswith("sha256:")
        with pytest.raises(FrozenInstanceError):
            projection.model_schema = "changed"
    assert projection.model_schema == "base"
    assert calls == ["active", "active"]
    assert all(not resolver.calls for resolver in fixture.resolver_factory.resolvers)


@pytest.mark.parametrize(
    "field",
    [
        "max_metadata_bytes",
        "max_generation_bytes",
        "max_catalog_rows",
        "max_definition_utf16_bytes",
        "max_dependency_rows",
        "profile-reference",
        "profile-subject",
        "platform-subject",
        "database",
        "control_authority",
        "control_connection_ref",
        "control_schema",
        "capacity_authority",
        "trusted_toolchain",
        "qualification_policy_id",
    ],
)
def test_registration_substitution_rejects_after_real_authentication(tmp_path, monkeypatch, field):
    fixture = _fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        claim = _claim(verifier, fixture)
        if field.startswith("max_"):
            claim = replace(claim, limits=replace(claim.limits, **{field: getattr(claim.limits, field) - 1}))
        elif field == "profile-reference":
            claim = replace(
                claim,
                trusted_profile=replace(claim.trusted_profile, reference=OriginalRef("other", "sha256:" + "a" * 64)),
            )
        elif field in {"profile-subject", "platform-subject"}:
            subject = replace(claim.platform_subject, platform_policy_sha256="sha256:" + "b" * 64)
            claim = replace(
                claim,
                **(
                    {"platform_subject": subject}
                    if field == "platform-subject"
                    else {"trusted_profile": replace(claim.trusted_profile, subject=subject)}
                ),
            )
        elif field == "database":
            pin = replace(claim.model_database, database_name="different")
            claim = replace(claim, model_database=pin, control_database=pin)
        elif field == "trusted_toolchain":
            claim = replace(claim, trusted_toolchain=registration_inputs()[field])
        elif field.endswith("authority"):
            claim = replace(claim, **{field: OriginalRef("other", "sha256:" + "a" * 64)})
        else:
            claim = replace(claim, **{field: "different"})
        with pytest.raises(ValueError, match="differs|differ"):
            _reader(verifier).read(fixture.refs, registration=claim)


@pytest.mark.parametrize("schema", ["dbo", "sys", "information_schema", "runtime_local", "dpone_control"])
def test_control_or_system_schema_cannot_be_model_schema(tmp_path, monkeypatch, schema):
    fixture = _fixture(
        tmp_path,
        monkeypatch,
        policy_change=lambda p: p["authoring_template"]["invocation_target"].update(schema=schema),
    )
    with _native_verifier(fixture, []) as verifier:
        with pytest.raises(ValueError, match="dedicated"):
            _reader(verifier).read(fixture.refs, registration=_claim(verifier, fixture))


def test_catalog_requires_explicit_invocation_target(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch, policy_change=lambda p: p.pop("authoring_template"))
    with _native_verifier(fixture, []) as verifier:
        with pytest.raises(ValueError, match="invocation_target"):
            _reader(verifier).read(fixture.refs, registration=_claim(verifier, fixture))


def test_changed_release_bytes_are_not_replaced_by_registration_claim(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        claim = _claim(verifier, fixture)
        path = fixture.release_root / "release-set.json"
        path.write_bytes(path.read_bytes() + b" ")
        with pytest.raises(ValueError, match="full-byte"):
            _reader(verifier).read(fixture.refs, registration=claim)


def test_no_caller_created_resolved_dto_or_proof_callback_is_accepted():
    with pytest.raises(ValueError, match="concrete"):
        MssqlPhysicalCatalogPolicyReader(verifier=lambda: True, documents=None)


def test_invalid_registration_rejects_before_original_acquisition(tmp_path, monkeypatch):
    from dpone.adapters.native_delivery_originals import NativeOriginalVerifier

    fixture = _fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:

        def forbidden_acquisition(*args, **kwargs):
            pytest.fail("invalid registration must not acquire originals")

        monkeypatch.setattr(NativeOriginalVerifier, "resolve", forbidden_acquisition)
        with pytest.raises(ValueError, match="exact physical registration"):
            _reader(verifier).read(fixture.refs, registration=object())


def test_changed_member_bytes_reject_before_pure_projection(tmp_path, monkeypatch):
    from dpone.adapters import dbt_mssql_physical_catalog_policy as policy_adapter

    fixture = _fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        claim = _claim(verifier, fixture)
        read_member = NativeProjectDocumentReader.read
        documents = NativeProjectDocumentReader(read_file=read_confined_file, max_policy_bytes=1024 * 1024)
        reader = MssqlPhysicalCatalogPolicyReader(verifier=verifier, documents=documents)

        def changed_member(self, *args, **kwargs):
            payload, intent = read_member(self, *args, **kwargs)
            return (payload + b" " if self is documents else payload), intent

        def forbidden_projection(**kwargs):
            pytest.fail("changed member bytes must not reach projection")

        monkeypatch.setattr(NativeProjectDocumentReader, "read", changed_member)
        monkeypatch.setattr(policy_adapter, "project_catalog_policy", forbidden_projection)
        with pytest.raises(ValueError, match="member changed after original verification"):
            reader.read(fixture.refs, registration=claim)


def test_projection_follows_real_authentication_and_member_read(tmp_path, monkeypatch):
    from dpone.adapters import dbt_mssql_physical_catalog_policy as policy_adapter
    from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
    from dpone.contracts.dbt_mssql_physical_catalog_binding import CatalogPolicyProjection

    fixture = _fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        claim = _claim(verifier, fixture)
        events = []
        resolve = NativeOriginalVerifier.resolve
        read_member = NativeProjectDocumentReader.read
        project = policy_adapter.project_catalog_policy
        documents = NativeProjectDocumentReader(read_file=read_confined_file, max_policy_bytes=1024 * 1024)
        reader = MssqlPhysicalCatalogPolicyReader(verifier=verifier, documents=documents)

        def acquire(self, *args, **kwargs):
            result = resolve(self, *args, **kwargs)
            events.append("authenticated")
            return result

        def members(self, *args, **kwargs):
            result = read_member(self, *args, **kwargs)
            if self is documents:
                events.append("members")
            return result

        def projection(**kwargs):
            assert events == ["authenticated", "members"]
            assert kwargs["policy_bytes"] == kwargs["original"].policy_document
            events.append("projection")
            return project(**kwargs)

        monkeypatch.setattr(NativeOriginalVerifier, "resolve", acquire)
        monkeypatch.setattr(NativeProjectDocumentReader, "read", members)
        monkeypatch.setattr(policy_adapter, "project_catalog_policy", projection)
        result = reader.read(fixture.refs, registration=claim)
        assert type(result) is CatalogPolicyProjection
        assert events == ["authenticated", "members", "projection"]


def test_policy_target_must_match_actual_effective_execution_target(tmp_path, monkeypatch):
    fixture = _fixture(
        tmp_path,
        monkeypatch,
        policy_change=lambda p: p["authoring_template"]["invocation_target"].update(schema="other"),
    )
    with _native_verifier(fixture, []) as verifier:
        with pytest.raises(ValueError, match="effective execution target"):
            _reader(verifier).read(fixture.refs, registration=_claim(verifier, fixture))


def test_explicit_retained_profile_subject_is_preserved(tmp_path, monkeypatch):
    from dpone.contracts.native_delivery_json import decode_native_delivery_json
    from dpone.contracts.native_originals import encode_native_original_subject

    retained = registration_inputs()["trusted_profile"].subject
    payload = decode_native_delivery_json(encode_native_original_subject(retained))
    fixture = _fixture(
        tmp_path,
        monkeypatch,
        policy_change=lambda p: p["native_execution"]["trusted_execution"].update(
            profile={"reference": {"locator": "retained/profile", "sha256": "sha256:" + "c" * 64}, "subject": payload}
        ),
    )
    with _native_verifier(fixture, []) as verifier:
        claim = _claim(verifier, fixture)
        projection = _reader(verifier).read(fixture.refs, registration=claim)
        assert projection.trusted_profile.subject == retained
        assert projection.trusted_profile.subject != projection.platform_subject
        with pytest.raises(ValueError, match="profile reference or subject"):
            _reader(verifier).read(
                fixture.refs,
                registration=replace(
                    claim, trusted_profile=replace(claim.trusted_profile, subject=claim.platform_subject)
                ),
            )


def test_member_reader_rejects_changed_policy_after_original_verification(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        claim = _claim(verifier, fixture)

        def changed_member(root, relative, *, max_bytes):
            if relative == "dpone/native-policy.json":
                path = root / relative
                path.write_bytes(path.read_bytes() + b" ")
            return read_confined_file(root, relative, max_bytes=max_bytes)

        reader = MssqlPhysicalCatalogPolicyReader(
            verifier=verifier,
            documents=NativeProjectDocumentReader(read_file=changed_member, max_policy_bytes=1024 * 1024),
        )
        with pytest.raises(ValueError, match="verified archive inventory"):
            reader.read(fixture.refs, registration=claim)


def test_model_alias_must_match_authenticated_execution_profile(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    with _native_verifier(fixture, []) as verifier:
        claim = replace(_claim(verifier, fixture), model_connection_ref="other-model")
        with pytest.raises(ValueError, match="model connection"):
            _reader(verifier).read(fixture.refs, registration=claim)


def test_unsupported_adapter_in_real_release_rejects_before_projection(tmp_path, monkeypatch):
    from dpone.contracts.dbt_contract_validation import canonical_fingerprint
    from dpone.readiness.dbt_airflow_execution_pack import DbtAirflowExecutionPackBuilder

    build = DbtAirflowExecutionPackBuilder.build

    def changed_pack(self, **kwargs):
        # Produce an invalid actual serialized member, not a fake verified result.
        # The real source reader must reject its unsupported adapter contract.
        execution = kwargs["execution_pack"]
        object.__setattr__(execution, "profile", replace(execution.profile, adapter_type="postgres"))
        unsigned = execution.to_dict()
        unsigned.pop("pack_sha256")
        object.__setattr__(execution, "pack_sha256", canonical_fingerprint(unsigned))
        return build(self, **kwargs)

    monkeypatch.setattr(DbtAirflowExecutionPackBuilder, "build", changed_pack)
    fixture = _fixture(tmp_path, monkeypatch)
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader

    with pytest.raises(ValueError) as failure:
        DbtReleaseSourceReader(bundle_operations=fixture.bundles, read_file=read_confined_file).read(
            fixture.release_root, expected_release_id=fixture.identity.release_id
        )
    causes = []
    error = failure.value
    while error is not None:
        causes.append(str(error))
        error = error.__cause__
    assert any("toolchain contract is unsupported" in message for message in causes)
    with _native_verifier(fixture, []) as verifier:
        with pytest.raises(ValueError, match="source_inventory"):
            _reader(verifier).read(fixture.refs, registration=MssqlPhysicalRuntimeRegistration(**registration_inputs()))
