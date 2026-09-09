"""Published integrity, evidence and promotion schemas for dbt self-service."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import (
    DIGEST,
    RELATIVE,
    TOKEN,
    nullable,
    object_schema,
)
from dpone.contracts.dbt_publish_schema_contract_evidence import (
    evidence_schema_contracts,
)


def promotion_schema_contracts() -> dict[str, dict[str, Any]]:
    """Return schemas owned by release evidence and multi-repo promotion."""

    return {
        **evidence_schema_contracts(),
        "dpone.dbt-release-integrity.v1": _release_integrity(),
        "dpone.dbt-release-materialization.v1": _release_materialization(),
        "dpone.dbt-prod-mirror-prepare.v1": _prod_mirror_prepare(),
        "dpone.dbt-prod-promotion.v1": _prod_promotion(with_evidence_set=False),
        "dpone.dbt-prod-promotion.v2": _prod_promotion(with_evidence_set=True),
        "dpone.dbt-prod-promotion.v3": _workspace_prod_promotion(),
        "dpone.dbt-workspace-mirror-prepare.v1": _workspace_mirror_prepare(),
        "dpone.dbt-workspace-promotion-verification.v1": _workspace_promotion_verification(),
        "dpone.dbt-promotion-verification.v1": _promotion_verification(),
    }


def _release_integrity() -> dict[str, Any]:
    return object_schema(
        (
            "schema",
            "passed",
            "subject_path",
            "subject_sha256",
            "file_count",
            "total_bytes",
            "no_op",
        ),
        {
            "schema": {"const": "dpone.dbt-release-integrity.v1"},
            "passed": {"const": True},
            "subject_path": RELATIVE,
            "subject_sha256": DIGEST,
            "file_count": {"type": "integer", "minimum": 1, "maximum": 50_000},
            "total_bytes": {
                "type": "integer",
                "minimum": 1,
                "maximum": 2 * 1024 * 1024 * 1024,
            },
            "no_op": {"type": "boolean"},
        },
    )


def _release_materialization() -> dict[str, Any]:
    return object_schema(
        ("schema", "passed", "release_id", "release_dir", "no_op"),
        {
            "schema": {"const": "dpone.dbt-release-materialization.v1"},
            "passed": {"const": True},
            "release_id": DIGEST,
            "release_dir": {
                "type": "string",
                "minLength": 1,
                "maxLength": 4096,
            },
            "no_op": {"type": "boolean"},
        },
    )


def _prod_mirror_prepare() -> dict[str, Any]:
    return object_schema(
        (
            "schema",
            "passed",
            "release_id",
            "promotion_id",
            "mirror_path",
            "source_snapshot_path",
            "descriptor_path",
            "no_op",
        ),
        {
            "schema": {"const": "dpone.dbt-prod-mirror-prepare.v1"},
            "passed": {"const": True},
            "release_id": DIGEST,
            "promotion_id": DIGEST,
            "mirror_path": RELATIVE,
            "source_snapshot_path": RELATIVE,
            "descriptor_path": RELATIVE,
            "no_op": {"type": "boolean"},
        },
    )


def _prod_promotion(
    *,
    with_evidence_set: bool,
) -> dict[str, Any]:
    safe_text = {
        "type": "string",
        "minLength": 1,
        "maxLength": 2048,
        "pattern": "^[^\\u0000-\\u001f\\u007f]+$",
    }
    required = [
        "schema",
        "release_id",
        "source_snapshot_path",
        "source_snapshot_sha256",
        "dev_deployment_id",
        "dev_evidence_ref",
        "dev_evidence_subject_sha256",
        "dev_evidence_artifact_name",
        "dev_evidence_producer_workflow",
        "dev_evidence_source_commit",
        "promotion_id",
    ]
    properties: dict[str, Any] = {
        "schema": {"const": ("dpone.dbt-prod-promotion.v2" if with_evidence_set else "dpone.dbt-prod-promotion.v1")},
        "release_id": DIGEST,
        "source_snapshot_path": RELATIVE,
        "source_snapshot_sha256": DIGEST,
        "dev_deployment_id": DIGEST,
        "dev_evidence_ref": safe_text,
        "dev_evidence_subject_sha256": DIGEST,
        "dev_evidence_artifact_name": deepcopy(safe_text),
        "dev_evidence_producer_workflow": deepcopy(safe_text),
        "dev_evidence_source_commit": {
            "type": "string",
            "pattern": "^[0-9a-f]{40}$",
        },
        "promotion_id": DIGEST,
    }
    if with_evidence_set:
        required.extend(
            (
                "dev_evidence_set_id",
                "dev_evidence_campaign_request_sha256",
            )
        )
        properties["dev_evidence_set_id"] = DIGEST
        properties["dev_evidence_campaign_request_sha256"] = DIGEST
    return object_schema(tuple(required), properties)


def _workspace_prod_promotion() -> dict[str, Any]:
    schema = _prod_promotion(with_evidence_set=True)
    schema["required"].append("mirror_root")
    schema["properties"]["schema"] = {"const": "dpone.dbt-prod-promotion.v3"}
    schema["properties"]["mirror_root"] = deepcopy(RELATIVE)
    return schema


def _workspace_mirror_prepare() -> dict[str, Any]:
    schema = _prod_mirror_prepare()
    schema["required"].remove("passed")
    schema["required"].remove("mirror_path")
    schema["required"].extend(("mirror_root", "projects"))
    del schema["properties"]["passed"]
    del schema["properties"]["mirror_path"]
    schema["properties"].update(
        {
            "schema": {"const": "dpone.dbt-workspace-mirror-prepare.v1"},
            "mirror_root": deepcopy(RELATIVE),
            "projects": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "uniqueItems": True,
                "items": deepcopy(RELATIVE),
            },
        }
    )
    return schema


def _workspace_promotion_verification() -> dict[str, Any]:
    schema = _promotion_verification()
    for key in ("expected_project_bundle_sha256", "observed_project_bundle_sha256"):
        schema["required"].remove(key)
        del schema["properties"][key]
    schema["required"].append("projects")
    schema["properties"].update(
        {
            "schema": {"const": "dpone.dbt-workspace-promotion-verification.v1"},
            "release_id": deepcopy(DIGEST),
            "source_snapshot_sha256": deepcopy(DIGEST),
            "projects": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": object_schema(
                    (
                        "project_path",
                        "expected_project_bundle_sha256",
                        "observed_project_bundle_sha256",
                        "passed",
                        "code",
                    ),
                    {
                        "project_path": deepcopy(RELATIVE),
                        "expected_project_bundle_sha256": deepcopy(DIGEST),
                        "observed_project_bundle_sha256": nullable(DIGEST),
                        "passed": {"type": "boolean"},
                        "code": deepcopy(TOKEN),
                    },
                ),
            },
        }
    )
    return schema


def _promotion_verification() -> dict[str, Any]:
    schema = object_schema(
        (
            "schema",
            "status",
            "passed",
            "code",
            "release_id",
            "source_snapshot_sha256",
            "expected_project_bundle_sha256",
            "observed_project_bundle_sha256",
        ),
        {
            "schema": {"const": "dpone.dbt-promotion-verification.v1"},
            "status": {"enum": ["passed", "failed"]},
            "passed": {"type": "boolean"},
            "code": TOKEN,
            "release_id": nullable(DIGEST),
            "source_snapshot_sha256": nullable(DIGEST),
            "expected_project_bundle_sha256": nullable(DIGEST),
            "observed_project_bundle_sha256": nullable(DIGEST),
        },
    )
    schema["oneOf"] = [
        {
            "properties": {
                "status": {"const": "passed"},
                "passed": {"const": True},
                "code": {"const": "DPONE_DBT_PROMOTION_SOURCE_VERIFIED"},
            },
            "required": ["status", "passed", "code"],
        },
        {
            "properties": {
                "status": {"const": "failed"},
                "passed": {"const": False},
                "code": {"const": "DPONE_DBT_PROMOTION_SOURCE_DRIFT"},
            },
            "required": ["status", "passed", "code"],
        },
    ]
    return schema


def _unique_tokens() -> dict[str, Any]:
    return {
        "type": "array",
        "uniqueItems": True,
        "items": TOKEN,
    }


__all__ = ["promotion_schema_contracts"]
