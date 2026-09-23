"""Pure phase-evidence obligations over an already type-validated record projection.

The worker owns enum/record validation and transition application. This policy
checks only which observed facts a phase requires, in the original error order;
it performs no I/O and supplies no proof or execution authority.
"""

from __future__ import annotations


def validate_phase_obligations(
    *,
    phase: int,
    sequence: int,
    object_present: bool,
    process_present: bool,
    exit_code: int | None,
    result_present: bool,
    verification_present: bool,
    error_present: bool,
    observation_present: bool,
    parent_present: bool,
) -> None:
    """Check the stable TdsAttemptPhase ordinal and exact fact-presence flags."""
    if phase < 7:
        index = phase
        if sequence < index or error_present or parent_present:
            raise ValueError("mssql_native.tds_invalid_phase_evidence")
        if (object_present) != (index >= 1) or (process_present) != (index >= 3):
            raise ValueError("mssql_native.tds_invalid_phase_identity")
        if (exit_code is not None) != (index >= 5) or (verification_present) != (index == 6):
            raise ValueError("mssql_native.tds_invalid_phase_result")
        if index < 5 and result_present:
            raise ValueError("mssql_native.tds_invalid_phase_result")
        if (observation_present) != (index >= 1):
            raise ValueError("mssql_native.tds_invalid_phase_observation")
        if index == 6 and (exit_code != 0 or not result_present):
            raise ValueError("mssql_native.tds_unverified_exit")
    else:
        prior = (
            6
            if verification_present
            else 5
            if exit_code is not None
            else 3
            if process_present
            else 1
            if object_present
            else 0
        )
        required = phase - 6 + prior
        successful_parent_retirement = verification_present and parent_present and not error_present
        failed_retirement = error_present
        if (not successful_parent_retirement and not failed_retirement) or sequence < required:
            raise ValueError("mssql_native.tds_missing_failure_authority")
        if phase != 7 and not observation_present:
            raise ValueError("mssql_native.tds_missing_retirement_proof")
        if (verification_present) != (parent_present):
            raise ValueError("mssql_native.tds_parent_unsettled")
    if process_present and not object_present:
        raise ValueError("mssql_native.tds_process_without_object")
    if result_present and exit_code is None:
        raise ValueError("mssql_native.tds_result_without_exit")
    if (exit_code is not None or result_present) and not process_present:
        raise ValueError("mssql_native.tds_result_without_process")
    if verification_present and (exit_code != 0 or not result_present):
        raise ValueError("mssql_native.tds_unverified_exit")
