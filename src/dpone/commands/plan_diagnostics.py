"""Translate recognized planning failures into actionable manifest diagnostics.

Unknown failures remain the caller's responsibility so programming errors are not
misclassified as user configuration mistakes.
"""

from __future__ import annotations

from dpone.manifest.errors import ManifestConfigurationError


def configuration_error(error: ValueError) -> ManifestConfigurationError | None:
    """Return a user-facing error only for explicitly supported planning codes."""
    code = str(error)
    if (
        code.startswith(("mssql_native.transport_invalid:", "mssql_native.transport_required:"))
        or code == "mssql_native.transport_requires_native_route"
    ):
        return ManifestConfigurationError(
            f"{code}: source.options.native_transfer.execution.native_chunks.transport "
            "requires backend: mssql_python|mssql_sqlclient, input: rows|arrow, and integer "
            "max_worker_address_space_bytes in 67108864..17179869184 for mssql_python "
            "or 8589934592..17179869184 for mssql_sqlclient; "
            "max_input_batch_bytes is SqlClient-only, in 1048576..268435456. "
            "Use documented finite bounds and operation_timeout_seconds >= startup_timeout_seconds. "
            "Use typed_binary/mssql_native with bounded_stream on ClickHouse -> MSSQL; "
            "omit transport to keep BCP."
        )
    if code not in (
        "mssql_native.invalid_limit:encoding_parallelism",
        "mssql_native.invalid_limit:import_parallelism",
    ):
        return None
    field = code.split(":", 1)[1]
    return ManifestConfigurationError(
        f"{code}: source.options.native_transfer.execution.native_chunks.{field} "
        "must be an integer in 1..64; omit the field to use chunking.parallelism."
    )
