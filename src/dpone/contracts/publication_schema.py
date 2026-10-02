"""Closed operator plan for the versioned publication catalog, without SQL I/O.

The digest confirms reviewed scope and DDL identity, not credentials, live
endpoint admission, or permission to change existing catalog objects.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import asdict, dataclass

from dpone.contracts.clickhouse_cluster_publication import canonical_json
from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding


@dataclass(frozen=True, slots=True)
class PublicationSchemaPlan:
    binding: PublicationAuthorityBinding
    endpoint_identity: str
    ddl_sha256: str
    catalog_version: int = 1
    contract: str = "dpone.publication-schema-plan.v1"

    def __post_init__(self) -> None:
        if (
            not isinstance(self.binding, PublicationAuthorityBinding)
            or type(self.catalog_version) is not int
            or self.catalog_version != 1
            or self.contract != "dpone.publication-schema-plan.v1"
            or any(
                not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
                for value in (self.endpoint_identity, self.ddl_sha256)
            )
        ):
            raise ValueError("invalid publication schema plan")

    @property
    def payload(self) -> str:
        return canonical_json(asdict(self))

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()

    def confirms(self, value: object) -> bool:
        """Reject malformed confirmations without a Unicode comparison error."""
        return (
            isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{64}", value) is not None
            and hmac.compare_digest(value, self.digest)
        )


def decode_schema_plan(raw: bytes) -> PublicationSchemaPlan:
    """Decode canonical private-plan bytes; reject extra, duplicate or stale fields."""
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 16 * 1024:
        raise ValueError("invalid publication schema plan")
    try:
        document = json.loads(raw.decode("utf-8"))
        if not isinstance(document, dict):
            raise ValueError("object required")
        document["binding"] = PublicationAuthorityBinding.from_mapping(document["binding"])
        result = PublicationSchemaPlan(**document)
        if result.payload.encode() != raw:
            raise ValueError("noncanonical plan")
        return result
    except (KeyError, TypeError, ValueError):
        raise ValueError("invalid publication schema plan") from None
