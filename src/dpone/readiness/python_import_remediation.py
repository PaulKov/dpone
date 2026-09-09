"""Shared redacted remediation policy for optional Python dependencies."""

from __future__ import annotations


def python_import_remediation(
    reason_code: str | None,
    *,
    not_installed: str,
    load_failed: str,
) -> str:
    """Return one stable fix hint without exposing probe inputs or exceptions."""

    fixes = {
        "python_import_name_invalid": "Use a valid dotted Python module name, then rerun the doctor",
        "python_import_not_installed": not_installed,
        "python_import_failed": load_failed,
        "python_import_path_invalid": "Repair or shorten the effective Python import path, then rerun the doctor",
        "python_import_policy_invalid": "Repair or reduce the effective Python startup policy, then rerun the doctor",
        "python_import_startup_unsupported": (
            "Use a standard reproducible Python startup environment, then rerun the doctor"
        ),
        "python_import_timed_out": "Investigate the timed-out Python import, then rerun the doctor",
        "python_import_probe_unavailable": (
            "Verify that the effective child Python interpreter can start, then rerun the doctor"
        ),
    }
    fallback = "Verify the requested Python runtime dependency, then rerun the doctor"
    return fixes.get(reason_code, fallback) if reason_code is not None else fallback


__all__ = ["python_import_remediation"]
