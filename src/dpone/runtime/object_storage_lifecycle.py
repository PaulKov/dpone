"""Object-storage lifecycle rule rendering and verification."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from dpone.storage import ObjectStorageUri

LIFECYCLE_SCHEMA = "dpone.object_storage.lifecycle_readiness.v1"


class ObjectStorageLifecycleAdvisor:
    def render(self, prefix: ObjectStorageUri, *, policy: Any) -> dict[str, object]:
        return {
            "schema_version": LIFECYCLE_SCHEMA,
            "uri_prefix": str(prefix.prefix()),
            "rules": [
                {
                    "ID": "dpone-stage-expire-staging-objects",
                    "Status": "Enabled",
                    "Filter": {"Prefix": prefix.prefix().key},
                    "Expiration": {"Days": policy.lifecycle_expiration_days},
                    "AbortIncompleteMultipartUpload": {
                        "DaysAfterInitiation": policy.abort_incomplete_multipart_days,
                    },
                }
            ],
        }

    def verify(
        self,
        prefix: ObjectStorageUri,
        *,
        policy: Any,
        existing_rules: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, object]:
        expected = self.render(prefix, policy=policy)["rules"][0]
        found = any(_compatible_rule(rule, expected) for rule in existing_rules)
        blockers = (
            ("object_storage_lifecycle_rule_missing",)
            if policy.require_lifecycle_rule == "required" and not found
            else ()
        )
        warnings = (
            ("object_storage_lifecycle_rule_missing",) if policy.require_lifecycle_rule == "warn" and not found else ()
        )
        return {
            "schema_version": LIFECYCLE_SCHEMA,
            "uri_prefix": str(prefix.prefix()),
            "passed": not blockers,
            "warnings": list(warnings),
            "blockers": list(blockers),
            "expected_rule": expected,
        }


def _compatible_rule(rule: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return (
        str(rule.get("Status")) == "Enabled"
        and _mapping(rule.get("Filter")).get("Prefix") == _mapping(expected.get("Filter")).get("Prefix")
        and int(_mapping(rule.get("Expiration")).get("Days") or 0)
        <= int(_mapping(expected.get("Expiration")).get("Days") or 0)
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = ["ObjectStorageLifecycleAdvisor"]
