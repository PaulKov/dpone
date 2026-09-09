from __future__ import annotations

from dpone.gitops.schema_contract_primitives import (
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    integer_schema,
    issue_ref,
    number_schema,
    object_schema,
    string_schema,
)


def airflow_admission_check_contract() -> object:
    return contract(
        name="airflow-admission-check",
        kind="gitops.airflow_admission_check",
        title="dpone GitOps Airflow admission check report",
        required=(
            "kind",
            "schema_version",
            "producer",
            "mode",
            "runner_policy",
            "artifact_dir",
            "manifest_path",
            "pod_spec_path",
            "timeout_seconds",
            "commands",
            "results",
            "warnings",
            "blockers",
        ),
        properties={
            "kind": const_schema("gitops.airflow_admission_check"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "mode": string_schema(),
            "runner_policy": string_schema(),
            "artifact_dir": string_schema(),
            "manifest_path": string_schema(),
            "pod_spec_path": string_schema(),
            "timeout_seconds": integer_schema(),
            "commands": array_schema(_command_schema()),
            "results": array_schema(_result_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def _command_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "kind", "command", "path", "required", "timeout_seconds", "executed"),
        properties={
            "name": string_schema(),
            "kind": string_schema(),
            "command": string_schema(),
            "path": string_schema(),
            "required": boolean_schema(),
            "timeout_seconds": integer_schema(),
            "executed": boolean_schema(),
        },
    )


def _result_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "command", "exit_code", "stdout", "stderr", "duration_seconds"),
        properties={
            "name": string_schema(),
            "command": string_schema(),
            "exit_code": integer_schema(),
            "stdout": string_schema(),
            "stderr": string_schema(),
            "duration_seconds": number_schema(),
        },
    )


__all__ = ["airflow_admission_check_contract"]
