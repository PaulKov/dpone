"""Safe operator remediation text for dbt runtime failures."""

from __future__ import annotations


def runtime_remediation(code: str) -> str:
    """Return the deterministic next action for one public failure code."""

    if code == "DPONE_DBT_TARGET_IDENTITY_MISMATCH":
        return (
            "The build did not start and the target was not mutated. Ask the "
            "platform owner to correct the deployment binding or promote a "
            "deployment matching the release logical database and schema."
        )
    if code in {"DPONE_DBT_INVOCATION_CONTEXT_INVALID", "DPONE_DBT_SELECTION_DRIFT"}:
        return (
            "The build did not start and the target was not mutated. Rebuild "
            "and publish a new release from the editable dev dbt source."
        )
    if code == "DPONE_DBT_PACK_INVALID":
        return (
            "Re-fetch the pinned release and deployment artifacts, verify "
            "their checksums, and never edit the generated execution pack."
        )
    if code == "DPONE_DBT_SCHEMA_DRIFT":
        return (
            "Keep transfers blocked, reconcile the live relation with the dbt "
            "contract, and publish a new immutable release when source semantics changed."
        )
    if code == "COMMIT_UNKNOWN":
        return (
            "Do not retry automatically. Reconcile the target and durable "
            "evidence, then obtain route-owner approval for the recovery action."
        )
    return (
        "Keep transfers blocked, inspect the durable workload evidence, and "
        "start a new attempt only after the reported cause is corrected."
    )
