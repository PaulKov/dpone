"""Closed retirement review scope; serialization is not deployment admission."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import asdict, dataclass

from dpone.contracts import clickhouse_cluster_publication as c
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding, publication_binding_digest
from dpone.contracts.publication_retirement import PublicationRetirementPlan, require_retirement_history
from dpone.contracts.publication_retirement_codec import decode_retirement_plan


@dataclass(frozen=True, slots=True)
class PublicationRetirementOperatorPlan:
    binding: PublicationAuthorityBinding
    endpoint_identity: str
    context_subject: str
    journal_identity: str
    retirement: PublicationRetirementPlan

    def __post_init__(self) -> None:
        for value in (self.endpoint_identity, self.context_subject, self.journal_identity):
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("canonical retirement scope digest required")
        if not isinstance(self.binding, PublicationAuthorityBinding):
            raise ValueError("exact retirement binding required")
        require_retirement_history(self.retirement)
        if publication_binding_digest(self.binding, endpoint_identity=self.endpoint_identity) != (
            self.retirement.observation.binding_digest
        ):
            raise ValueError("retirement authority binding differs")

    @property
    def payload(self) -> str:
        return c.canonical_json(
            {
                "contract": "dpone.publication-retirement-operator-plan.v1",
                "binding": asdict(self.binding),
                "endpoint_identity": self.endpoint_identity,
                "context_subject": self.context_subject,
                "journal_identity": self.journal_identity,
                "retirement": json.loads(self.retirement.payload),
            }
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()

    def confirms(self, value: object) -> bool:
        return (
            isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{64}", value) is not None
            and hmac.compare_digest(value, self.digest)
        )


def decode_retirement_operator_plan(raw: bytes) -> PublicationRetirementOperatorPlan:
    """Keep original provenance exact, including expired historical observations."""
    try:
        if not isinstance(raw, bytes) or not 0 < len(raw) <= 1024 * 1024:
            raise ValueError("bounded bytes required")
        document = json.loads(raw.decode("utf-8"))
        if (
            not isinstance(document, dict)
            or document.pop("contract") != "dpone.publication-retirement-operator-plan.v1"
        ):
            raise ValueError("unsupported retirement operator version")
        document["binding"] = PublicationAuthorityBinding.from_mapping(document["binding"])
        document["retirement"] = decode_retirement_plan(c.canonical_json(document["retirement"]).encode())
        result = PublicationRetirementOperatorPlan(**document)
        if result.payload.encode() != raw:
            raise ValueError("noncanonical retirement operator plan")
        return result
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError, OverflowError, c.ClusterPublicationError):
        raise ValueError("invalid retirement operator plan") from None
