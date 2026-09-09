"""Text and Markdown rendering for execution-plan contracts."""

from __future__ import annotations


def render_external_target_contract_text(physical_design: dict) -> list[str]:
    """Render the external target authority in plain text."""

    contract = physical_design.get("external_contract") or {}
    if not contract:
        return []
    unique_index = contract.get("unique_index") or {}
    columns = ",".join(str(item) for item in unique_index.get("columns") or ())
    collations = contract.get("text_key_collations") or {}
    collation_text = ",".join(f"{key}={value}" for key, value in collations.items()) or "none"
    return [
        (
            "- target_provisioning: "
            f"{physical_design.get('provisioning')} authority={physical_design.get('ddl_authority')}"
        ),
        f"- target_unique_index: required={unique_index.get('required')} columns={columns}",
        f"- target_text_key_collations: {collation_text}",
    ]


def render_external_target_contract_md(physical_design: dict) -> list[str]:
    """Render the external target authority in Markdown."""

    contract = physical_design.get("external_contract") or {}
    if not contract:
        return []
    unique_index = contract.get("unique_index") or {}
    columns = ", ".join(str(item) for item in unique_index.get("columns") or ())
    collations = contract.get("text_key_collations") or {}
    collation_text = ", ".join(f"{key}={value}" for key, value in collations.items()) or "none"
    return [
        (
            f"- target_provisioning: `{physical_design.get('provisioning')}` "
            f"authority=`{physical_design.get('ddl_authority')}`"
        ),
        f"- target_unique_index: required=`{unique_index.get('required')}` columns=`{columns}`",
        f"- target_text_key_collations: `{collation_text}`",
    ]


def render_transport_contract_text(contract: dict) -> list[str]:
    if not contract:
        return []
    return [
        f"- native_transfer_transport: {contract.get('route')} lossless={contract.get('lossless')}",
        f"- native_transfer_wire_format: {contract.get('wire_format')}",
        f"- native_transfer_text_codec: {contract.get('text_codec')}",
    ]


def render_transport_contract_md(contract: dict) -> list[str]:
    return [
        f"- route: `{contract.get('route')}`",
        f"- lossless: `{contract.get('lossless')}`",
        f"- wire_format: `{contract.get('wire_format')}`",
        f"- text_codec: `{contract.get('text_codec')}`",
        f"- null_policy: `{contract.get('null_policy')}`",
        f"- empty_string_policy: `{contract.get('empty_string_policy')}`",
    ]


def render_replay_contract_text(contract: dict) -> list[str]:
    if not contract:
        return []
    return [
        f"- native_transfer_replay: {contract.get('route')} lossless={contract.get('lossless_replay')}",
        f"- native_transfer_state_boundary: {contract.get('state_boundary')}",
        f"- native_transfer_state_commit: {contract.get('state_commit')}",
        f"- native_transfer_delete_mode: {contract.get('delete_mode')}",
        f"- native_transfer_idempotency: {contract.get('idempotency')}",
    ]


def render_replay_contract_md(contract: dict) -> list[str]:
    return [
        f"- route: `{contract.get('route')}`",
        f"- lossless_replay: `{contract.get('lossless_replay')}`",
        f"- state_boundary: `{contract.get('state_boundary')}`",
        f"- state_commit: `{contract.get('state_commit')}`",
        f"- delete_mode: `{contract.get('delete_mode')}`",
        f"- idempotency: `{contract.get('idempotency')}`",
    ]


def render_evidence_contract_text(contract: dict) -> list[str]:
    if not contract:
        return []
    artifacts = contract.get("required_artifacts") or []
    checks = contract.get("required_checks") or []
    partition_retry = contract.get("partition_retry") or {}
    return [
        f"- native_transfer_evidence: artifacts={len(artifacts)} checks={len(checks)}",
        f"- native_transfer_partition_retry: {partition_retry.get('enabled')}",
    ]


def render_evidence_contract_md(contract: dict) -> list[str]:
    partition_retry = contract.get("partition_retry") or {}
    artifact_lines = [f"  - `{artifact}`" for artifact in contract.get("required_artifacts") or []]
    check_lines = [f"  - `{check}`" for check in contract.get("required_checks") or []]
    return [
        f"- route: `{contract.get('route')}`",
        f"- strategy: `{contract.get('strategy')}`",
        f"- state_commit_gate: `{contract.get('state_commit_gate')}`",
        f"- partition_retry: `{partition_retry.get('enabled')}`",
        "- required_artifacts:",
        *artifact_lines,
        "- required_checks:",
        *check_lines,
    ]


__all__ = [
    "render_evidence_contract_md",
    "render_evidence_contract_text",
    "render_external_target_contract_md",
    "render_external_target_contract_text",
    "render_replay_contract_md",
    "render_replay_contract_text",
    "render_transport_contract_md",
    "render_transport_contract_text",
]
