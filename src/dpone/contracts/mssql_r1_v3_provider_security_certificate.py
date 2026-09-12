"""Certificate values, secret templates and receipt identity for MSSQL R1."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import TypeAlias

from dpone.contracts.mssql_r1_v3_codec import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ResourceKindV1,
)
from dpone.contracts.mssql_r1_v3_provider_security_enums import (
    MssqlR1CertificateCreationProfileV1,
    MssqlR1CertificateSignatureAlgorithmV1,
    MssqlR1SecretTemplateKindV1,
    MssqlR1SecurityCanonicalModel,
    require_exact_digest,
)
from dpone.contracts.mssql_r1_v3_provider_security_principals import (
    MSSQL_R1_SECURITY_PRINCIPAL_ARMS,
    SECRET_PLACEHOLDER,
    MssqlR1BindingSignerInstancePrincipalRefV1,
    MssqlR1EnvironmentPrincipalRefV1,
    MssqlR1SecurityPrincipalRefV1,
    MssqlR1SharedSignerPrincipalRefV1,
    decode_security_principal,
    require_exact_identifier,
)
from dpone.contracts.mssql_r1_v3_schema_security import (
    MssqlR1AuthenticationTypeV3,
    MssqlR1PrincipalTypeV3,
    MssqlR1SubjectRoleV3,
)

_METADATA = b"dpone-r1-security-shared-certificate-metadata-v1\0"
_TEMPLATE = b"dpone-r1-security-secret-sql-template-v1\0"
_OBSERVATION = b"dpone-r1-security-certificate-catalog-observation-v1\0"
_IDENTITY = b"dpone-r1-security-certificate-install-identity-ref-v2\0"
_RECEIPT_COORDINATE = (
    MssqlR1ResourceKindV1.STATIC_OBJECT,
    "dpone_authority",
    "dpone_provider_install_receipt_v3",
    None,
)
_SHARED_SUBJECTS = {"dpone R1 V3 attestor", "dpone R1 V3 stage owner"}
_BRACKETED = r"\[((?:[^\]\r\n]|\]\])+)\]"
_PASSWORD = re.escape(SECRET_PLACEHOLDER)
_CREATE_TEMPLATE = re.compile(
    rf"CREATE CERTIFICATE (?P<certificate>{_BRACKETED}) ENCRYPTION BY PASSWORD = '(?P<secret>{_PASSWORD})';\n\Z"
)
_SIGN_TEMPLATE = re.compile(
    rf"ADD SIGNATURE TO (?P<schema>{_BRACKETED})\.(?P<object>{_BRACKETED}) "
    rf"BY CERTIFICATE (?P<certificate>{_BRACKETED}) WITH PASSWORD = '(?P<secret>{_PASSWORD})';\n\Z"
)


def _digest(payload: bytes) -> bytes:
    return hashlib.sha256(payload).digest()


def _date(value: object, field: str) -> str:
    if type(value) is not str or len(value) != 8 or not value.isascii() or not value.isdigit():
        raise MssqlR1V3ContractError(f"{field} is not an exact YYYYMMDD date")
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise MssqlR1V3ContractError(f"{field} is not a calendar date") from exc
    return value


def _digests(value: object, field: str) -> tuple[bytes, ...]:
    if type(value) is not tuple:
        raise MssqlR1V3ContractError(f"{field} is not an exact tuple")
    result = tuple(require_exact_digest(item, field) for item in value)
    if len(set(result)) != len(result):
        raise MssqlR1V3ContractError(f"{field} contains duplicates")
    return result


def _subject(value: object, field: str) -> str:
    if type(value) is not str or not value:
        raise MssqlR1V3ContractError(f"{field} is not exact text")
    if unicodedata.normalize("NFC", value) != value or any(char in value for char in "\r\n\0"):
        raise MssqlR1V3ContractError(f"{field} is not canonical text")
    if len(value.encode("utf-16-le")) // 2 > 64:
        raise MssqlR1V3ContractError(f"{field} exceeds 64 UTF-16 code units")
    return value


def _unquote_identifier(value: str, field: str) -> str:
    return require_exact_identifier(value[1:-1].replace("]]", "]"), field)


@dataclass(frozen=True, slots=True)
class MssqlR1SharedCertificateMetadataV1(MssqlR1SecurityCanonicalModel):
    creation_profile: MssqlR1CertificateCreationProfileV1
    signature_algorithm: MssqlR1CertificateSignatureAlgorithmV1
    subject: str
    start_date_yyyymmdd: str
    expiry_date_yyyymmdd: str
    owner: MssqlR1EnvironmentPrincipalRefV1

    def __post_init__(self) -> None:
        if (
            type(self.creation_profile) is not MssqlR1CertificateCreationProfileV1
            or (self.creation_profile is not MssqlR1CertificateCreationProfileV1.SQLSERVER_SELF_SIGNED_SHA2_256_V1)
            or type(self.signature_algorithm) is not MssqlR1CertificateSignatureAlgorithmV1
            or (self.signature_algorithm is not MssqlR1CertificateSignatureAlgorithmV1.SHA2_256)
        ):
            raise MssqlR1V3ContractError("certificate algorithm authority is inexact")
        _subject(self.subject, "certificate subject")
        _date(self.start_date_yyyymmdd, "start date")
        _date(self.expiry_date_yyyymmdd, "expiry date")
        if (
            self.subject not in _SHARED_SUBJECTS
            or self.start_date_yyyymmdd != "20000101"
            or self.expiry_date_yyyymmdd != "99991231"
            or type(self.owner) is not MssqlR1EnvironmentPrincipalRefV1
            or (self.owner.subject_role is not MssqlR1SubjectRoleV3.PROVISIONER)
        ):
            raise MssqlR1V3ContractError("shared certificate metadata is outside the exact R1 profile")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _METADATA,
            (
                self.creation_profile,
                self.signature_algorithm,
                self.subject,
                self.start_date_yyyymmdd,
                self.expiry_date_yyyymmdd,
                self.owner.canonical_bytes,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SharedCertificateMetadataV1:
        values = list(decode_canonical_bytes(payload, _METADATA, field_count=6))
        values[0] = expect_enum(MssqlR1CertificateCreationProfileV1, values[0], "creation profile")
        values[1] = expect_enum(MssqlR1CertificateSignatureAlgorithmV1, values[1], "signature algorithm")
        values[5] = decode_security_principal(expect_bytes(values[5], "certificate owner"))
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SecretSqlTemplateV1(MssqlR1SecurityCanonicalModel):
    template_kind: MssqlR1SecretTemplateKindV1
    sql_template_utf8_bytes: bytes
    secret_policy_digest: bytes

    def __post_init__(self) -> None:
        if (
            type(self.template_kind) is not MssqlR1SecretTemplateKindV1
            or type(self.sql_template_utf8_bytes) is not bytes
        ):
            raise MssqlR1V3ContractError("secret template discriminator or payload is inexact")
        require_exact_digest(self.secret_policy_digest, "secret policy digest")
        try:
            text = self.sql_template_utf8_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MssqlR1V3ContractError("secret SQL template is not UTF-8") from exc
        if unicodedata.normalize("NFC", text) != text or "\r" in text or "\0" in text:
            raise MssqlR1V3ContractError("secret SQL template grammar is invalid")
        pattern = (
            _CREATE_TEMPLATE if self.template_kind is MssqlR1SecretTemplateKindV1.CREATE_CERTIFICATE else _SIGN_TEMPLATE
        )
        match = pattern.fullmatch(text)
        if match is None:
            raise MssqlR1V3ContractError("secret SQL template is outside the closed R1 grammar")
        for field in ("certificate", "schema", "object"):
            value = match.groupdict().get(field)
            if value is not None:
                _unquote_identifier(value, f"template {field}")

    @property
    def resolved_identifiers(self) -> tuple[str, ...]:
        text = self.sql_template_utf8_bytes.decode("utf-8")
        pattern = (
            _CREATE_TEMPLATE if self.template_kind is MssqlR1SecretTemplateKindV1.CREATE_CERTIFICATE else _SIGN_TEMPLATE
        )
        match = pattern.fullmatch(text)
        if match is None:  # pragma: no cover - constructor invariant
            raise MssqlR1V3ContractError("secret template invariant was violated")
        order = (
            ("certificate",)
            if self.template_kind is MssqlR1SecretTemplateKindV1.CREATE_CERTIFICATE
            else (
                "schema",
                "object",
                "certificate",
            )
        )
        return tuple(_unquote_identifier(match.group(name), f"template {name}") for name in order)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_TEMPLATE, (self.template_kind, self.sql_template_utf8_bytes, self.secret_policy_digest))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SecretSqlTemplateV1:
        kind, sql, digest = decode_canonical_bytes(payload, _TEMPLATE, field_count=3)
        return cls(
            expect_enum(MssqlR1SecretTemplateKindV1, kind, "template kind"),
            expect_bytes(sql, "template bytes"),
            expect_bytes(digest, "secret policy digest"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1CertificateCatalogObservationV1(MssqlR1SecurityCanonicalModel):
    certificate_name: str
    certificate_exists: bool
    certificate_subject: str | None
    start_date_yyyymmdd: str | None
    expiry_date_yyyymmdd: str | None
    certificate_owner: MssqlR1SecurityPrincipalRefV1 | None
    certificate_thumbprint: bytes | None
    private_key_present: bool | None
    certificate_user_name: str | None
    certificate_user_exists: bool
    certificate_user_type: MssqlR1PrincipalTypeV3 | None
    authentication_type: MssqlR1AuthenticationTypeV3 | None
    certificate_user_sid_bytes: bytes | None
    certificate_user_bound_thumbprint: bytes | None
    ordered_module_signature_digests: tuple[bytes, ...]
    ordered_permission_edge_digests: tuple[bytes, ...]

    def __post_init__(self) -> None:
        require_exact_identifier(self.certificate_name, "certificate name")
        if type(self.certificate_exists) is not bool or type(self.certificate_user_exists) is not bool:
            raise MssqlR1V3ContractError("certificate existence flags are inexact")
        certificate = (
            self.certificate_subject,
            self.start_date_yyyymmdd,
            self.expiry_date_yyyymmdd,
            self.certificate_owner,
            self.certificate_thumbprint,
            self.private_key_present,
        )
        user = (
            self.certificate_user_name,
            self.certificate_user_type,
            self.authentication_type,
            self.certificate_user_sid_bytes,
            self.certificate_user_bound_thumbprint,
        )
        if self.certificate_exists != all(item is not None for item in certificate) or (
            self.certificate_user_exists != all(item is not None for item in user)
        ):
            raise MssqlR1V3ContractError("certificate observation presence is inconsistent")
        if self.certificate_exists:
            _subject(self.certificate_subject, "observed certificate subject")
            _date(self.start_date_yyyymmdd, "observed start date")
            _date(self.expiry_date_yyyymmdd, "observed expiry date")
            if (
                type(self.certificate_owner) not in {contract for _, contract in MSSQL_R1_SECURITY_PRINCIPAL_ARMS}
                or type(self.certificate_thumbprint) is not bytes
                or not self.certificate_thumbprint
                or (type(self.private_key_present) is not bool)
            ):
                raise MssqlR1V3ContractError("certificate catalog identity is incomplete")
        if self.certificate_user_exists:
            require_exact_identifier(self.certificate_user_name, "certificate user name")
            if (
                type(self.certificate_user_type) is not MssqlR1PrincipalTypeV3
                or self.certificate_user_type is not MssqlR1PrincipalTypeV3.CERTIFICATE
                or type(self.authentication_type) is not MssqlR1AuthenticationTypeV3
                or (self.authentication_type is not MssqlR1AuthenticationTypeV3.NONE)
                or type(self.certificate_user_sid_bytes) is not bytes
                or not self.certificate_user_sid_bytes
                or (
                    type(self.certificate_user_bound_thumbprint) is not bytes
                    or not self.certificate_user_bound_thumbprint
                )
            ):
                raise MssqlR1V3ContractError("certificate user identity is invalid")
        signatures = _digests(self.ordered_module_signature_digests, "signature digests")
        permissions = _digests(self.ordered_permission_edge_digests, "permission digests")
        if not (self.certificate_exists and self.certificate_user_exists) and (signatures or permissions):
            raise MssqlR1V3ContractError("partial certificate cannot carry authority edges")

    @property
    def canonical_bytes(self) -> bytes:
        values = tuple(getattr(self, name) for name in self.__dataclass_fields__)
        owner = None if self.certificate_owner is None else self.certificate_owner.canonical_bytes
        return canonical_bytes(_OBSERVATION, (*values[:5], owner, *values[6:]))

    @property
    def is_installed_public_key_only(self) -> bool:
        return bool(
            self.certificate_exists
            and self.certificate_user_exists
            and self.private_key_present is False
            and self.certificate_thumbprint == self.certificate_user_bound_thumbprint
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1CertificateCatalogObservationV1:
        values = list(decode_canonical_bytes(payload, _OBSERVATION, field_count=16))
        if values[5] is not None:
            values[5] = decode_security_principal(expect_bytes(values[5], "certificate owner"))
        if values[10] is not None:
            values[10] = expect_enum(MssqlR1PrincipalTypeV3, values[10], "principal type")
        if values[11] is not None:
            values[11] = expect_enum(MssqlR1AuthenticationTypeV3, values[11], "authentication type")
        for index in (14, 15):
            values[index] = tuple(
                expect_bytes(item, "authority digest") for item in expect_tuple(values[index], "digests")
            )
        return cls(*values)  # type: ignore[arg-type]


MssqlR1CertificateSignerRefV2: TypeAlias = (
    MssqlR1SharedSignerPrincipalRefV1 | MssqlR1BindingSignerInstancePrincipalRefV1
)


@dataclass(frozen=True, slots=True)
class MssqlR1CertificateInstallIdentityRefV2(MssqlR1SecurityCanonicalModel):
    receipt_resource_ref: MssqlR1PhysicalResourceRefV1
    installation_effect_key: bytes
    receipt_payload_digest: bytes
    stable_catalog_attestation_digest: bytes
    signer_ref: MssqlR1CertificateSignerRefV2
    lifecycle_policy_digest: bytes
    pinned_certificate_observation_digest: bytes

    def __post_init__(self) -> None:
        if (
            type(self.receipt_resource_ref) is not MssqlR1PhysicalResourceRefV1
            or (
                (
                    self.receipt_resource_ref.resource_kind,
                    self.receipt_resource_ref.schema_name,
                    self.receipt_resource_ref.object_name,
                    self.receipt_resource_ref.coordinate_name,
                )
                != _RECEIPT_COORDINATE
            )
            or type(self.signer_ref)
            not in {MssqlR1SharedSignerPrincipalRefV1, MssqlR1BindingSignerInstancePrincipalRefV1}
        ):
            raise MssqlR1V3ContractError("certificate install identity authority is inexact")
        for name in (
            "installation_effect_key",
            "receipt_payload_digest",
            "stable_catalog_attestation_digest",
            "lifecycle_policy_digest",
            "pinned_certificate_observation_digest",
        ):
            require_exact_digest(getattr(self, name), name)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _IDENTITY,
            (
                self.receipt_resource_ref.canonical_bytes,
                self.installation_effect_key,
                self.receipt_payload_digest,
                self.stable_catalog_attestation_digest,
                self.signer_ref.canonical_bytes,
                self.lifecycle_policy_digest,
                self.pinned_certificate_observation_digest,
            ),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1CertificateInstallIdentityRefV2:
        values = list(decode_canonical_bytes(payload, _IDENTITY, field_count=7))
        values[0] = MssqlR1PhysicalResourceRefV1.from_canonical_bytes(expect_bytes(values[0], "receipt resource"))
        values[4] = decode_security_principal(expect_bytes(values[4], "signer ref"))
        return cls(*values)  # type: ignore[arg-type]
