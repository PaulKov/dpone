"""Vault-backed protected bootstrap authority for semantic refresh V2.

Deployment planning precedes the MSSQL activation transaction, so its trust
cannot be read from rows that the plan itself will create.  This adapter uses a
version-pinned Vault KV snapshot as the independent pre-deployment authority.
Logical DagRun admission uses a separate create-once path derived from the
receipt digest, allowing new runs without mutating deployment authority.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunAdmissionAuthority,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256

if TYPE_CHECKING:
    from dpone.contracts.dbt_semantic_refresh_deployment_contracts import (
        SemanticRefreshDeploymentAuthoritySubject,
    )
    from dpone.contracts.semantic_refresh_route_certification import (
        SemanticRefreshRouteLiveCertificationReceipt,
    )
    from dpone.contracts.semantic_refresh_runtime_assurance import (
        SemanticRefreshRuntimeAssuranceReceipt,
    )

_INDEX_SCHEMA = "dpone.semantic-refresh-vault-authority-index.v1"
_RUN_SCHEMA = "dpone.semantic-refresh-vault-run-authority.v1"
_DIGEST_PREFIX = "sha256:"


class _VaultKvClient(Protocol):
    def get_secret(self, *, mount_point: str, path: str) -> Mapping[str, Any]: ...


class SemanticRefreshVaultAuthorityError(RuntimeError):
    """Raised when protected Vault authority is unavailable or malformed."""


@dataclass(frozen=True, slots=True)
class _AuthorityIndex:
    deployment_subject_sha256: str
    route_certification_receipt_sha256: str
    runtime_assurance_receipt_sha256s: tuple[str, ...]
    seal_policy_authority_sha256s: tuple[str, ...]
    vault_version: int


class VaultSemanticRefreshAuthorityIndexReader:
    """Read one exact immutable deployment authority snapshot from Vault KV v2."""

    def __init__(
        self,
        *,
        client: _VaultKvClient,
        mount_point: str,
        path: str,
        expected_version: int,
    ) -> None:
        if not mount_point or not path:
            raise ValueError("Vault semantic-refresh authority mount and path are required")
        if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version <= 0:
            raise ValueError("Vault semantic-refresh authority version must be positive")
        self._client = client
        self._mount_point = mount_point
        self._path = path
        self._expected_version = expected_version

    def load(self) -> _AuthorityIndex:
        try:
            raw = self._client.get_secret(mount_point=self._mount_point, path=self._path)
            return _parse_authority_index(raw, expected_version=self._expected_version)
        except SemanticRefreshVaultAuthorityError:
            raise
        except Exception as exc:
            raise SemanticRefreshVaultAuthorityError("protected Vault deployment authority is unavailable") from exc

    def authorizes_seal_policy(self, authority_sha256: str) -> bool:
        """Return whether one exact policy document is pinned by this index."""

        try:
            return _digest(authority_sha256, "seal policy authority") in self.load().seal_policy_authority_sha256s
        except (ValueError, SemanticRefreshVaultAuthorityError):
            return False


@dataclass(frozen=True, slots=True)
class VaultSemanticRefreshDeploymentVerifier:
    """Verify the exact release/deployment/model/baseline subject."""

    reader: VaultSemanticRefreshAuthorityIndexReader

    def verify(self, subject: SemanticRefreshDeploymentAuthoritySubject) -> bool:
        try:
            return self.reader.load().deployment_subject_sha256 == subject.subject_sha256
        except (AttributeError, SemanticRefreshVaultAuthorityError):
            return False


@dataclass(frozen=True, slots=True)
class VaultSemanticRefreshAssuranceVerifier:
    """Verify route and runtime receipts against one pinned deployment index."""

    reader: VaultSemanticRefreshAuthorityIndexReader
    now: Callable[[], datetime]

    def verify_route(
        self,
        receipt: SemanticRefreshRouteLiveCertificationReceipt,
        *,
        expected_coordinate_sha256: str,
    ) -> bool:
        try:
            index = self.reader.load()
            coordinate = semantic_refresh_sha256(receipt.coordinates.to_dict())
            return bool(
                index.route_certification_receipt_sha256 == receipt.route_certification_receipt_sha256
                and coordinate == expected_coordinate_sha256
                and receipt.authorizes(
                    self.now(),
                    trusted_receipt_sha256=index.route_certification_receipt_sha256,
                )
            )
        except (AttributeError, ValueError, SemanticRefreshVaultAuthorityError):
            return False

    def verify_runtime(self, receipt: SemanticRefreshRuntimeAssuranceReceipt) -> bool:
        try:
            index = self.reader.load()
            return bool(
                receipt.runtime_assurance_receipt_sha256 in index.runtime_assurance_receipt_sha256s
                and receipt.authorizes(
                    self.now(),
                    trusted_digest=receipt.runtime_assurance_receipt_sha256,
                    expected_subject=receipt.subject,
                )
            )
        except (AttributeError, ValueError, SemanticRefreshVaultAuthorityError):
            return False


class VaultSemanticRefreshRunAdmissionVerifier:
    """Verify one create-once logical DagRun authority document in Vault."""

    def __init__(
        self,
        *,
        client: _VaultKvClient,
        mount_point: str,
        path_prefix: str,
    ) -> None:
        if not mount_point or not path_prefix or path_prefix.endswith("/"):
            raise ValueError("Vault run-authority mount and prefix are required")
        self._client = client
        self._mount_point = mount_point
        self._path_prefix = path_prefix

    def verify(self, authority: SemanticRefreshRunAdmissionAuthority) -> bool:
        try:
            suffix = _digest(authority.authority_receipt_sha256, "authority_receipt_sha256")[7:]
            raw = self._client.get_secret(
                mount_point=self._mount_point,
                path=f"{self._path_prefix}/{suffix}",
            )
            expected = semantic_refresh_vault_run_authority(authority)
            return _closed_without_metadata(raw) == expected and _vault_version(raw) == 1
        except Exception:
            return False


def semantic_refresh_vault_authority_index(
    *,
    deployment_subject_sha256: str,
    route_certification_receipt_sha256: str,
    runtime_assurance_receipt_sha256s: Sequence[str],
    seal_policy_authority_sha256s: Sequence[str],
) -> dict[str, object]:
    """Build canonical non-secret values for one create-once Vault KV document."""

    assurances = tuple(
        sorted({_digest(value, "runtime assurance receipt") for value in runtime_assurance_receipt_sha256s})
    )
    if not assurances:
        raise ValueError("Vault authority index requires runtime assurance receipts")
    seal_policies = tuple(sorted({_digest(value, "seal policy authority") for value in seal_policy_authority_sha256s}))
    if not seal_policies:
        raise ValueError("Vault authority index requires seal policy authorities")
    unsigned: dict[str, object] = {
        "deployment_subject_sha256": _digest(deployment_subject_sha256, "deployment_subject_sha256"),
        "route_certification_receipt_sha256": _digest(
            route_certification_receipt_sha256,
            "route_certification_receipt_sha256",
        ),
        "runtime_assurance_receipt_sha256s": list(assurances),
        "seal_policy_authority_sha256s": list(seal_policies),
        "schema": _INDEX_SCHEMA,
    }
    return {**unsigned, "authority_index_sha256": semantic_refresh_sha256(unsigned)}


def semantic_refresh_vault_run_authority(
    authority: SemanticRefreshRunAdmissionAuthority,
) -> dict[str, object]:
    """Build the exact create-once Vault document for one logical DagRun."""

    if not isinstance(authority, SemanticRefreshRunAdmissionAuthority):
        raise TypeError("run authority must be a canonical SemanticRefreshRunAdmissionAuthority")
    unsigned = {
        "plan_bundle_sha256": authority.plan_bundle_sha256,
        "schema": _RUN_SCHEMA,
        "workflow_execution_id": authority.workflow_execution_id,
    }
    expected_digest = semantic_refresh_sha256(unsigned)
    if authority.authority_receipt_sha256 != expected_digest:
        raise ValueError("run authority receipt digest differs from the Vault create-once formula")
    return {**unsigned, "authority_receipt_sha256": expected_digest}


def _parse_authority_index(raw: Mapping[str, object], *, expected_version: int) -> _AuthorityIndex:
    values = _closed_without_metadata(raw)
    expected_fields = {
        "authority_index_sha256",
        "deployment_subject_sha256",
        "route_certification_receipt_sha256",
        "runtime_assurance_receipt_sha256s",
        "seal_policy_authority_sha256s",
        "schema",
    }
    if set(values) != expected_fields or values.get("schema") != _INDEX_SCHEMA:
        raise SemanticRefreshVaultAuthorityError("Vault deployment authority fields are not closed")
    unsigned = {key: value for key, value in values.items() if key != "authority_index_sha256"}
    if values.get("authority_index_sha256") != semantic_refresh_sha256(unsigned):
        raise SemanticRefreshVaultAuthorityError("Vault deployment authority digest differs")
    assurances_raw = values.get("runtime_assurance_receipt_sha256s")
    if not isinstance(assurances_raw, list):
        raise SemanticRefreshVaultAuthorityError("Vault runtime assurance closure must be an array")
    assurances = tuple(_digest(value, "runtime assurance receipt") for value in assurances_raw)
    if not assurances or assurances != tuple(sorted(set(assurances))):
        raise SemanticRefreshVaultAuthorityError("Vault runtime assurance closure is not canonical")
    seal_policies_raw = values.get("seal_policy_authority_sha256s")
    if not isinstance(seal_policies_raw, list):
        raise SemanticRefreshVaultAuthorityError("Vault seal policy closure must be an array")
    seal_policies = tuple(_digest(value, "seal policy authority") for value in seal_policies_raw)
    if not seal_policies or seal_policies != tuple(sorted(set(seal_policies))):
        raise SemanticRefreshVaultAuthorityError("Vault seal policy closure is not canonical")
    version = _vault_version(raw)
    if version != expected_version:
        raise SemanticRefreshVaultAuthorityError("Vault deployment authority version differs")
    return _AuthorityIndex(
        deployment_subject_sha256=_digest(values["deployment_subject_sha256"], "deployment_subject_sha256"),
        route_certification_receipt_sha256=_digest(
            values["route_certification_receipt_sha256"],
            "route_certification_receipt_sha256",
        ),
        runtime_assurance_receipt_sha256s=assurances,
        seal_policy_authority_sha256s=seal_policies,
        vault_version=version,
    )


def _closed_without_metadata(raw: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(raw, Mapping) or "_metadata" not in raw:
        raise SemanticRefreshVaultAuthorityError("Vault authority snapshot lacks version metadata")
    return {str(key): value for key, value in raw.items() if key != "_metadata"}


def _vault_version(raw: Mapping[str, object]) -> int:
    metadata = raw.get("_metadata")
    if not isinstance(metadata, Mapping):
        raise SemanticRefreshVaultAuthorityError("Vault authority version metadata is invalid")
    version = metadata.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise SemanticRefreshVaultAuthorityError("Vault authority version must be positive")
    return version


def _digest(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith(_DIGEST_PREFIX)
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a canonical lowercase sha256 digest")
    return value


__all__ = [
    "SemanticRefreshVaultAuthorityError",
    "VaultSemanticRefreshAssuranceVerifier",
    "VaultSemanticRefreshAuthorityIndexReader",
    "VaultSemanticRefreshDeploymentVerifier",
    "VaultSemanticRefreshRunAdmissionVerifier",
    "semantic_refresh_vault_authority_index",
    "semantic_refresh_vault_run_authority",
]
