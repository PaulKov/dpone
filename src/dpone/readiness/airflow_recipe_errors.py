"""Stable error, remediation, and exit-code policy for recipe self-service."""

from __future__ import annotations

from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix

_USAGE_ERROR_CODES = frozenset(
    {
        "DPONE_AUTHORING_MODE_UNSUPPORTED",
        "DPONE_DOMAIN_ID_INVALID",
        "DPONE_RECIPE_AUTHORING_MODE_UNSUPPORTED",
        "DPONE_RECIPE_OPTION_UNSUPPORTED",
        "DPONE_RECIPE_REF_INVALID",
        "DPONE_ROUTE_FILTER_INVALID",
    }
)
_SAFETY_ERROR_CODES = frozenset(
    {
        "DPONE_RECIPE_ANSWERS_UNSAFE",
        "DPONE_RECIPE_CATALOG_UNTRUSTED",
        "DPONE_RECIPE_DIGEST_MISMATCH",
        "DPONE_RECIPE_SOURCE_CHANGED_DURING_BUILD",
    }
)


def recipe_error(
    code: str,
    message: str,
    *,
    stage: str,
    recipe_ref: str,
    entity: dict[str, str] | None = None,
) -> dict[str, object]:
    """Build a data-safe ``dpone.error.v1`` payload for one recipe failure."""

    return dpone_error(
        code,
        message,
        stage=stage,
        entity=entity or {"kind": "recipe", "id": recipe_ref},
        fixes=[_recipe_fix(code, recipe_ref=recipe_ref)],
        docs_url=error_docs_url(code),
    )


def recipe_error_exit_code(code: str) -> int:
    """Map recipe failures to the stable self-service CLI exit contract."""

    if code in _SAFETY_ERROR_CODES:
        return 4
    if code in _USAGE_ERROR_CODES:
        return 2
    return 1


def _recipe_fix(code: str, *, recipe_ref: str) -> dict[str, str]:
    if code in {"DPONE_RECIPE_REF_INVALID", "DPONE_RECIPE_NOT_FOUND"}:
        return manual_fix("choose_exact_recipe_ref", command="dpone recipe list")
    if code == "DPONE_RECIPE_PROFILE_NOT_ALLOWED":
        return manual_fix("choose_allowed_profile", command=f"dpone recipe show {recipe_ref}")
    if code in {
        "DPONE_RECIPE_ANSWERS_UNSAFE",
        "DPONE_RECIPE_OVERRIDE_FORBIDDEN",
        "DPONE_RECIPE_PARAMETER_REQUIRED",
        "DPONE_RECIPE_PARAMETER_SCHEMA_INVALID",
    }:
        return manual_fix("inspect_recipe_parameters", command=f"dpone recipe show {recipe_ref}")
    if code in {
        "DPONE_RECIPE_DIGEST_MISMATCH",
        "DPONE_RECIPE_SOURCE_CHANGED_DURING_BUILD",
    }:
        return manual_fix("restore_or_publish_new_version", command="dpone recipe validate")
    if code in {"DPONE_RECIPE_CATALOG_INVALID", "DPONE_RECIPE_CATALOG_NOT_CONFIGURED"}:
        return manual_fix("validate_recipe_catalog", command="dpone recipe validate")
    if code == "DPONE_RECIPE_CATALOG_UNTRUSTED":
        return manual_fix("ask_platform_owner_to_trust_catalog")
    if code == "DPONE_RECIPE_AUTHORING_MODE_UNSUPPORTED":
        return manual_fix("use_flow_authoring", command="dpone init pipeline --help")
    if code == "DPONE_AUTHORING_MODE_UNSUPPORTED":
        return manual_fix("choose_supported_authoring_mode", command="dpone init pipeline --help")
    if code == "DPONE_RECIPE_OPTION_UNSUPPORTED":
        return manual_fix("remove_external_recipe_options", command="dpone init pipeline --help")
    if code == "DPONE_DOMAIN_ID_INVALID":
        return manual_fix("use_canonical_domain_id", command="dpone init pipeline --help")
    if code == "DPONE_RECIPE_HERMETIC_TEST_UNSUPPORTED":
        return manual_fix("choose_hermetic_recipe", command="dpone recipe show " + recipe_ref)
    if code == "DPONE_PIPELINE_DOMAIN_MISMATCH":
        return manual_fix("use_recipe_domain", command=f"dpone recipe show {recipe_ref}")
    if code in {"DPONE_PIPELINE_LOCATOR_INVALID", "DPONE_PIPELINE_KEY_INVALID"}:
        return manual_fix("review_pipeline_locator_syntax", command="dpone init pipeline --help")
    if code in {"DPONE_RECIPE_ANSWERS_INVALID", "DPONE_RECIPE_ANSWERS_FIELD_FORBIDDEN"}:
        return manual_fix("review_pipeline_answers_contract", command="dpone init pipeline --help")
    if code == "DPONE_ROUTE_FILTER_INVALID":
        return manual_fix("choose_supported_route_filter", command="dpone recipe list --help")
    return manual_fix("inspect_recipe_contract", command="dpone recipe validate")


__all__ = ["recipe_error", "recipe_error_exit_code"]
