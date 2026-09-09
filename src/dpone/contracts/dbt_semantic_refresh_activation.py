"""Protected authority-persistence acknowledgement for V2 pack activation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.semantic_refresh_baseline_receipt import (
    SemanticRefreshBaselineAdoptionReceipt,
)
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_evidence_common import require_utc_timestamp
from dpone.contracts.semantic_refresh_route_certification import (
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_runtime_assurance import (
    SemanticRefreshRuntimeAssuranceReceipt,
)

ACTIVATION_AUTHORITY_RECEIPT_SCHEMA = "dpone.dbt-semantic-refresh-activation-authority-receipt.v1"
_BASE_ASSURANCE_KINDS = {"ddl_freeze", "writer_exclusivity"}
_DATETIME_ASSURANCE_KINDS = _BASE_ASSURANCE_KINDS | {"utc_semantics"}
_MSSQL_DATETIME2_6_UTC = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z$")


@dataclass(frozen=True, slots=True)
class SemanticRefreshActivationAuthorityReceipt:
    """Create-only protected-store acknowledgement for all runtime authorities."""

    release_id: str
    deployment_id: str
    plan_bundle_sha256: str
    authority_store_ref: str
    baseline_receipts: tuple[tuple[str, str], ...]
    route_certification_receipt_sha256: str
    runtime_assurance_receipts: tuple[tuple[str, str, str], ...]
    persisted_at: str
    activation_authority_receipt_sha256: str
    schema: str = ACTIVATION_AUTHORITY_RECEIPT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != ACTIVATION_AUTHORITY_RECEIPT_SCHEMA:
            raise SemanticRefreshContractError("activation authority receipt schema is unsupported")
        values = _activation_receipt_payload(
            release_id=self.release_id,
            deployment_id=self.deployment_id,
            plan_bundle_sha256=self.plan_bundle_sha256,
            authority_store_ref=self.authority_store_ref,
            baseline_receipts=self.baseline_receipts,
            route_certification_receipt_sha256=self.route_certification_receipt_sha256,
            runtime_assurance_receipts=self.runtime_assurance_receipts,
            persisted_at=self.persisted_at,
        )
        require_digest(
            self.activation_authority_receipt_sha256,
            "activation_authority_receipt_sha256",
        )
        if self.activation_authority_receipt_sha256 != semantic_refresh_sha256(values):
            raise SemanticRefreshContractError("activation authority receipt digest differs")

    @classmethod
    def build(
        cls,
        *,
        release_id: str,
        deployment_id: str,
        plan_bundle_sha256: str,
        authority_store_ref: str,
        baseline_receipts: tuple[tuple[str, str], ...],
        route_certification_receipt_sha256: str,
        runtime_assurance_receipts: tuple[tuple[str, str, str], ...],
        persisted_at: str,
    ) -> SemanticRefreshActivationAuthorityReceipt:
        canonical_baselines = _baseline_receipts(baseline_receipts)
        canonical_assurances = _runtime_assurance_receipts(runtime_assurance_receipts)
        values = _activation_receipt_payload(
            release_id=release_id,
            deployment_id=deployment_id,
            plan_bundle_sha256=plan_bundle_sha256,
            authority_store_ref=authority_store_ref,
            baseline_receipts=canonical_baselines,
            route_certification_receipt_sha256=route_certification_receipt_sha256,
            runtime_assurance_receipts=canonical_assurances,
            persisted_at=persisted_at,
        )
        return cls(
            release_id=str(values["release_id"]),
            deployment_id=str(values["deployment_id"]),
            plan_bundle_sha256=str(values["plan_bundle_sha256"]),
            authority_store_ref=str(values["authority_store_ref"]),
            baseline_receipts=canonical_baselines,
            route_certification_receipt_sha256=str(values["route_certification_receipt_sha256"]),
            runtime_assurance_receipts=canonical_assurances,
            persisted_at=str(values["persisted_at"]),
            activation_authority_receipt_sha256=semantic_refresh_sha256(values),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "activation_authority_receipt_sha256": self.activation_authority_receipt_sha256,
            "authority_store_ref": self.authority_store_ref,
            "baseline_receipts": [list(item) for item in self.baseline_receipts],
            "deployment_id": self.deployment_id,
            "persisted_at": self.persisted_at,
            "plan_bundle_sha256": self.plan_bundle_sha256,
            "release_id": self.release_id,
            "route_certification_receipt_sha256": self.route_certification_receipt_sha256,
            "runtime_assurance_receipts": [list(item) for item in self.runtime_assurance_receipts],
            "schema": self.schema,
        }


class SemanticRefreshProtectedAssuranceVerifierPort(Protocol):
    """Authenticate full protected receipts before deployment-plan compilation."""

    def verify_route(
        self,
        receipt: SemanticRefreshRouteLiveCertificationReceipt,
        *,
        expected_coordinate_sha256: str,
    ) -> bool: ...

    def verify_runtime(self, receipt: SemanticRefreshRuntimeAssuranceReceipt) -> bool: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshReleaseTemplateSubject:
    """Exact release/template coordinates authenticated before activation."""

    release_id: str
    template_pack_fingerprint: str
    pre_release_bundle_sha256: str
    package_artifacts_sha256: str

    def __post_init__(self) -> None:
        for field_name in (
            "release_id",
            "template_pack_fingerprint",
            "pre_release_bundle_sha256",
            "package_artifacts_sha256",
        ):
            require_digest(getattr(self, field_name), field_name)


class SemanticRefreshReleaseTemplateVerifierPort(Protocol):
    """Authenticate one template fingerprint against its protected release."""

    def verify(self, subject: SemanticRefreshReleaseTemplateSubject) -> bool: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshActivationAuthoritySet:
    """Retry-stable full authority plan that must be stored create-only.

    ``persisted_at`` is sampled once while planning and the same typed value is
    reused after an acknowledgement loss; apply-time clock sampling is invalid.
    """

    release_id: str
    deployment_id: str
    plan_bundle_sha256: str
    baselines: tuple[SemanticRefreshBaselineAdoptionReceipt, ...]
    route_certification: SemanticRefreshRouteLiveCertificationReceipt
    runtime_assurances: tuple[SemanticRefreshRuntimeAssuranceReceipt, ...]
    persisted_at: str

    def __post_init__(self) -> None:
        require_digest(self.release_id, "release_id")
        require_digest(self.deployment_id, "deployment_id")
        require_digest(self.plan_bundle_sha256, "plan_bundle_sha256")
        _activation_persisted_at(self.persisted_at)
        if not isinstance(self.route_certification, SemanticRefreshRouteLiveCertificationReceipt):
            raise SemanticRefreshContractError("route_certification must be a full typed receipt")
        baseline_pairs = tuple(
            (item.model_unique_id, item.baseline_adoption_receipt_sha256)
            for item in self.baselines
            if isinstance(item, SemanticRefreshBaselineAdoptionReceipt)
        )
        if len(baseline_pairs) != len(self.baselines):
            raise SemanticRefreshContractError("baselines must be full typed receipts")
        _baseline_receipts(baseline_pairs)
        assurance_triples = tuple(
            (
                item.subject.model_unique_id,
                item.subject.assurance_kind.value,
                item.runtime_assurance_receipt_sha256,
            )
            for item in self.runtime_assurances
            if isinstance(item, SemanticRefreshRuntimeAssuranceReceipt)
        )
        if len(assurance_triples) != len(self.runtime_assurances):
            raise SemanticRefreshContractError("runtime_assurances must be full typed receipts")
        _runtime_assurance_receipts(assurance_triples)

    @property
    def baseline_receipts(self) -> tuple[tuple[str, str], ...]:
        return tuple((item.model_unique_id, item.baseline_adoption_receipt_sha256) for item in self.baselines)

    @property
    def runtime_assurance_receipts(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(
            (
                item.subject.model_unique_id,
                item.subject.assurance_kind.value,
                item.runtime_assurance_receipt_sha256,
            )
            for item in self.runtime_assurances
        )


class SemanticRefreshActivationAuthorityStorePort(Protocol):
    """Persist every full authority receipt atomically with create-only semantics."""

    def persist_exact(
        self, authority: SemanticRefreshActivationAuthoritySet
    ) -> SemanticRefreshActivationAuthorityReceipt: ...


class SemanticRefreshDeploymentAuthorityLoaderPort(Protocol):
    """Load the exact persisted deployment receipt for worker-time admission."""

    def load_exact(
        self,
        *,
        release_id: str,
        deployment_id: str,
        plan_bundle_sha256: str,
    ) -> SemanticRefreshActivationAuthorityReceipt: ...


def _baseline_receipts(values: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    if not values:
        raise SemanticRefreshContractError("baseline receipt closure must be non-empty")
    result = tuple(
        (require_text(model, "baseline model_unique_id"), require_digest(digest, "baseline receipt digest"))
        for model, digest in values
    )
    if result != tuple(sorted(set(result))) or len({item[0] for item in result}) != len(result):
        raise SemanticRefreshContractError("baseline receipt closure must be canonical and unique by model")
    return result


def _activation_receipt_payload(
    *,
    release_id: str,
    deployment_id: str,
    plan_bundle_sha256: str,
    authority_store_ref: str,
    baseline_receipts: tuple[tuple[str, str], ...],
    route_certification_receipt_sha256: str,
    runtime_assurance_receipts: tuple[tuple[str, str, str], ...],
    persisted_at: str,
) -> dict[str, object]:
    return {
        "authority_store_ref": require_text(authority_store_ref, "authority_store_ref"),
        "baseline_receipts": [list(item) for item in _baseline_receipts(baseline_receipts)],
        "deployment_id": require_digest(deployment_id, "deployment_id"),
        "persisted_at": _activation_persisted_at(persisted_at),
        "plan_bundle_sha256": require_digest(plan_bundle_sha256, "plan_bundle_sha256"),
        "release_id": require_digest(release_id, "release_id"),
        "route_certification_receipt_sha256": require_digest(
            route_certification_receipt_sha256,
            "route_certification_receipt_sha256",
        ),
        "runtime_assurance_receipts": [list(item) for item in _runtime_assurance_receipts(runtime_assurance_receipts)],
        "schema": ACTIVATION_AUTHORITY_RECEIPT_SCHEMA,
    }


def _runtime_assurance_receipts(
    values: tuple[tuple[str, str, str], ...],
) -> tuple[tuple[str, str, str], ...]:
    result = tuple(
        (
            require_text(model, "runtime assurance model_unique_id"),
            require_text(kind, "runtime assurance kind"),
            require_digest(digest, "runtime assurance digest"),
        )
        for model, kind, digest in values
    )
    if not result or result != tuple(sorted(set(result))):
        raise SemanticRefreshContractError("runtime assurance receipt closure must be canonical and unique")
    model_ids = {item[0] for item in result}
    if any(
        {item[1] for item in result if item[0] == model} not in (_BASE_ASSURANCE_KINDS, _DATETIME_ASSURANCE_KINDS)
        for model in model_ids
    ):
        raise SemanticRefreshContractError("runtime assurance receipt closure must contain every exact model axis")
    return result


def _activation_persisted_at(value: object) -> str:
    """Require a UTC timestamp that round-trips through MSSQL ``datetime2(6)``."""

    timestamp = require_utc_timestamp(value, "persisted_at")
    if _MSSQL_DATETIME2_6_UTC.fullmatch(timestamp) is None:
        raise SemanticRefreshContractError(
            "persisted_at must use YYYY-MM-DDTHH:MM:SS with at most six fractional digits for MSSQL datetime2(6)"
        )
    return timestamp


__all__ = [
    "ACTIVATION_AUTHORITY_RECEIPT_SCHEMA",
    "SemanticRefreshActivationAuthoritySet",
    "SemanticRefreshActivationAuthorityStorePort",
    "SemanticRefreshActivationAuthorityReceipt",
    "SemanticRefreshDeploymentAuthorityLoaderPort",
    "SemanticRefreshProtectedAssuranceVerifierPort",
    "SemanticRefreshReleaseTemplateSubject",
    "SemanticRefreshReleaseTemplateVerifierPort",
]
