"""Collection-safe RED probe for Provider Attestation V2 evidence."""

from __future__ import annotations

import json
from typing import Any, Final

PROVIDER_ATTESTATION_OBSERVATION_PROPERTY: Final = "dpone.provider_attestation.v2.case_observation"


class ProviderAttestationMissingBehavior(Exception):
    """Support-only RED signal that owned production behavior is absent."""

    def __init__(self, case_id: str, diagnostic_class: str) -> None:
        self.case_id = case_id
        self.diagnostic_class = diagnostic_class
        super().__init__(case_id)


def observation_jcs(
    case_id: str,
    observed_behavior: str,
    reason: str | None,
    diagnostic_class: str | None,
) -> str:
    """Serialize one case observation as RFC 8785 JCS UTF-8 text."""

    return json.dumps(
        {
            "case_id": case_id,
            "diagnostic_class": diagnostic_class,
            "observed_behavior": observed_behavior,
            "reason": reason,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def record_observation(
    record_property: Any,
    case_id: str,
    observed_behavior: str,
    reason: str | None,
    diagnostic_class: str | None,
) -> None:
    record_property(
        PROVIDER_ATTESTATION_OBSERVATION_PROPERTY,
        observation_jcs(case_id, observed_behavior, reason, diagnostic_class),
    )


def run_case(case_id: str, record_property: Any) -> None:
    """Execute one registry case after collection, without top-level production imports."""

    try:
        from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_oracle import execute_case

        behavior, reason = execute_case(case_id)
    except (ImportError, ModuleNotFoundError, AttributeError):
        record_observation(record_property, case_id, "not_observed", None, "missing_behavior")
        raise ProviderAttestationMissingBehavior(case_id, "missing_behavior") from None
    record_observation(record_property, case_id, behavior, reason, None)
