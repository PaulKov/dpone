"""Reviewed vendor and runtime identity policy."""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from .contract import VENDOR_FIELDS, WorkflowBinding, fail

RELEASE_IMAGE_DIGESTS: Mapping[str, str] = MappingProxyType(
    {
        "postgresql": "sha256:44c4ee9810eff91f7eab4d822642e01115b1a9eccce4bcbdde7604752d68eac6",
        "postgis": "sha256:b193e996618e9e632e2c6e268462b350c28a9c871cb0352b32905fc01e0299bd",
        "mssql": "sha256:ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89",
    }
)


def parse_vendor_policy(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != set(VENDOR_FIELDS):
        fail("inventory.vendors_invalid")
    normalized: dict[str, Any] = {}
    for section, fields in VENDOR_FIELDS.items():
        value = raw.get(section)
        if not isinstance(value, dict) or set(value) != set(fields):
            fail(f"inventory.vendor_fields_invalid:{section}")
        normalized[section] = {}
        for field in sorted(fields):
            item = value.get(field)
            if not isinstance(item, str) or not item.strip():
                fail(f"inventory.vendor_value_invalid:{section}:{field}")
            if field == "image_digest" and re.fullmatch(r"sha256:[0-9a-f]{64}", item) is None:
                fail(f"inventory.vendor_digest_invalid:{section}")
            normalized[section][field] = item
    return normalized


def enforce_image_digest_allowlist(
    policy: Mapping[str, Any],
    *,
    allowed: Mapping[str, str] = RELEASE_IMAGE_DIGESTS,
) -> None:
    """Require each observed database image to match the reviewed release pin."""

    required = frozenset({"postgresql", "postgis", "mssql"})
    if frozenset(allowed) != required:
        fail("inventory.vendor_image_allowlist_invalid")
    for section in sorted(required):
        observed = policy.get(section)
        if not isinstance(observed, Mapping) or observed.get("image_digest") != allowed[section]:
            fail(f"inventory.vendor_image_not_allowed:{section}")


def validate_vendor_metadata(
    raw: Any,
    *,
    suite_id: str,
    binding: WorkflowBinding,
    expected: dict[str, Any],
) -> None:
    if not isinstance(raw, dict):
        fail(f"evidence.vendors_required:{suite_id}")
    if set(raw) != set(VENDOR_FIELDS):
        fail(f"evidence.vendor_sections_mismatch:{suite_id}")
    for section, fields in VENDOR_FIELDS.items():
        value = raw.get(section)
        if not isinstance(value, dict):
            fail(f"evidence.vendor_section_required:{suite_id}:{section}")
        expected_keys = set(fields) | ({"source_sha"} if section == "runtime" else set())
        if set(value) != expected_keys:
            fail(f"evidence.vendor_fields_mismatch:{suite_id}:{section}")
        for field in fields:
            if not isinstance(value.get(field), str) or not value[field].strip():
                fail(f"evidence.vendor_field_required:{suite_id}:{section}:{field}")
        if (
            "image_digest" in fields
            and re.fullmatch(
                r"sha256:[0-9a-f]{64}",
                str(value["image_digest"]),
            )
            is None
        ):
            fail(f"evidence.vendor_digest_invalid:{suite_id}:{section}")
        for field in fields:
            if value[field] != expected[section][field]:
                fail(f"evidence.vendor_value_mismatch:{suite_id}:{section}:{field}")
    if raw["runtime"]["source_sha"] != binding.commit_sha:
        fail(f"evidence.vendor_source_sha_mismatch:{suite_id}")
