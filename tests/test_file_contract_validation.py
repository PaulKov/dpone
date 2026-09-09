from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.etl.file_contract_validation import (
    FileContractValidationError,
    project_key_snapshot_schema_contract,
    require_file_contract_validation,
    validate_mssql_delimited_file_contract,
)
from dpone.runtime.file_artifacts import FileExportArtifact


def _contract() -> SchemaContract:
    return SchemaContract.from_config(
        {
            "enforcement": "strict",
            "columns": {
                "id": {"type": "integer", "nullable": False},
                "value": {"type": "string", "nullable": False},
            },
        }
    )


def _artifact(tmp_path, body: bytes, *, codec: BulkTextCodec | None = None) -> FileExportArtifact:
    path = tmp_path / "payload.bcp"
    path.write_bytes(body)
    return FileExportArtifact(
        str(path),
        ["id", "value"],
        format="mssql-delimited",
        bulk_text_codec=codec,
        rows_exported=body.count(b"\n"),
    )


def test_receipt_binds_bytes_wire_schema_and_accepts_encoded_empty_string(tmp_path) -> None:
    codec = BulkTextCodec()
    artifact = _artifact(
        tmp_path,
        f"1\t{codec.empty_string_marker}\n2\tvalue\n".encode(),
        codec=codec,
    )
    schema = (("id", "int"), ("value", "nvarchar(max)"))

    receipt = validate_mssql_delimited_file_contract(
        artifact,
        schema=schema,
        contract=_contract(),
    )

    assert receipt.rows_validated == 2
    assert receipt.validated_schema == schema
    assert receipt.wire_contract_sha256 == artifact.wire_contract().sha256
    assert (
        require_file_contract_validation(
            artifact,
            _contract(),
            schema=schema,
        )
        is receipt
    )


def test_receipt_rejects_copy_null_but_not_bulk_codec_empty_string(tmp_path) -> None:
    artifact = _artifact(tmp_path, b"1\t\n", codec=BulkTextCodec())

    with pytest.raises(FileContractValidationError, match="not_null_violation"):
        validate_mssql_delimited_file_contract(
            artifact,
            schema=(("id", "int"), ("value", "nvarchar(max)")),
            contract=_contract(),
        )


def test_key_snapshot_contract_projection_is_explicit_and_does_not_weaken_full_validation(
    tmp_path,
) -> None:
    contract = _contract()
    projection = project_key_snapshot_schema_contract(
        contract,
        key_columns=("id",),
        source_schema=(("id", "integer"), ("value", "text")),
    )
    path = tmp_path / "keys.bcp"
    path.write_bytes(b"1\n")
    artifact = FileExportArtifact(
        str(path),
        ["id"],
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=1,
    )

    receipt = validate_mssql_delimited_file_contract(
        artifact,
        schema=(("id", "int"),),
        contract=projection,
    )

    assert tuple((projection.columns or {}).keys()) == ("id",)
    assert projection.enforcement == contract.enforcement
    assert tuple((contract.columns or {}).keys()) == ("id", "value")
    assert receipt.rows_validated == 1
    with pytest.raises(FileContractValidationError, match="contract_column_missing:value"):
        validate_mssql_delimited_file_contract(
            artifact,
            schema=(("id", "int"),),
            contract=contract,
        )


@pytest.mark.parametrize(
    ("key_columns", "source_schema", "contract", "blocker"),
    (
        (("missing",), (("id", "integer"),), _contract(), "key_column_missing_from_source:missing"),
        (("id", "ID"), (("id", "integer"),), _contract(), "key_column_duplicate:ID"),
        (
            ("id",),
            (("id", "integer"),),
            SchemaContract.from_config({"columns": {"value": {"nullable": False}}}),
            "key_column_missing_from_contract:id",
        ),
    ),
)
def test_key_snapshot_contract_projection_rejects_non_authoritative_keys(
    key_columns,
    source_schema,
    contract,
    blocker,
) -> None:
    with pytest.raises(FileContractValidationError, match=blocker):
        project_key_snapshot_schema_contract(
            contract,
            key_columns=key_columns,
            source_schema=source_schema,
        )


@pytest.mark.parametrize(
    ("body", "contract_config", "blocker"),
    (
        (
            b"not-an-integer\tvalue\n",
            {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "integer", "nullable": False},
                    "value": {"type": "string", "nullable": False},
                },
            },
            "logical_value_violation:id:schema.type_mismatch",
        ),
        (
            b"123.456\tvalue\n",
            {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "decimal", "precision": 5, "scale": 2, "nullable": False},
                    "value": {"type": "string", "nullable": False},
                },
            },
            "logical_value_violation:id:schema.type_mismatch",
        ),
    ),
)
def test_receipt_validates_decoded_logical_values_before_staging(
    tmp_path,
    body,
    contract_config,
    blocker,
) -> None:
    artifact = _artifact(tmp_path, body, codec=BulkTextCodec())

    with pytest.raises(FileContractValidationError, match=blocker):
        validate_mssql_delimited_file_contract(
            artifact,
            schema=(("id", "nvarchar(max)"), ("value", "nvarchar(max)")),
            contract=SchemaContract.from_config(contract_config),
        )


def test_receipt_decodes_text_empty_marker_and_binary_hex_for_shared_validator(tmp_path) -> None:
    codec = BulkTextCodec()
    path = tmp_path / "binary.bcp"
    path.write_bytes(f"{codec.empty_string_marker}\t00ff\n".encode())
    artifact = FileExportArtifact(
        str(path),
        ["value", "payload"],
        format="mssql-delimited",
        bulk_text_codec=codec,
        rows_exported=1,
    )
    contract = SchemaContract.from_config(
        {
            "enforcement": "strict",
            "columns": {
                "value": {"type": "string", "nullable": False},
                "payload": {"type": "binary", "nullable": False},
            },
        }
    )

    receipt = validate_mssql_delimited_file_contract(
        artifact,
        schema=(("value", "nvarchar(max)"), ("payload", "varbinary(max)")),
        contract=contract,
    )

    assert receipt.rows_validated == 1


def test_receipt_decodes_empty_binary_marker_without_conflating_copy_null(tmp_path) -> None:
    codec = BulkTextCodec()
    path = tmp_path / "empty-binary.bcp"
    path.write_bytes(f"{codec.empty_string_marker}\n".encode())
    artifact = FileExportArtifact(
        str(path),
        ["payload"],
        format="mssql-delimited",
        bulk_text_codec=codec,
        rows_exported=1,
    )
    contract = SchemaContract.from_config(
        {"enforcement": "strict", "columns": {"payload": {"type": "binary", "nullable": False}}}
    )

    receipt = validate_mssql_delimited_file_contract(
        artifact,
        schema=(("payload", "varbinary(max)"),),
        contract=contract,
    )

    assert receipt.rows_validated == 1


@pytest.mark.parametrize("unsupported", ("regex", "minimum", "maximum"))
def test_unimplemented_column_constraints_fail_authoring_instead_of_being_ignored(unsupported) -> None:
    with pytest.raises(ValueError, match=f"unsupported options: {unsupported}"):
        SchemaContract.from_config(
            {
                "columns": {
                    "value": {"type": "string", unsupported: ".*"},
                },
            }
        )


def test_receipt_requires_exact_bulk_text_codec_and_final_row_terminator(tmp_path) -> None:
    without_codec = _artifact(tmp_path, b"1\tvalue\n")
    with pytest.raises(FileContractValidationError, match="bulk_text_codec_required"):
        validate_mssql_delimited_file_contract(
            without_codec,
            schema=(("id", "int"), ("value", "nvarchar(max)")),
            contract=_contract(),
        )

    unterminated = _artifact(tmp_path, b"1\tvalue", codec=BulkTextCodec())
    with pytest.raises(FileContractValidationError, match="row_terminator_mismatch"):
        validate_mssql_delimited_file_contract(
            unterminated,
            schema=(("id", "int"), ("value", "nvarchar(max)")),
            contract=_contract(),
        )


def test_receipt_rejects_schema_digest_or_expected_schema_drift(tmp_path) -> None:
    schema = (("id", "int"), ("value", "nvarchar(max)"))
    artifact = _artifact(tmp_path, b"1\tvalue\n", codec=BulkTextCodec())
    receipt = validate_mssql_delimited_file_contract(
        artifact,
        schema=schema,
        contract=_contract(),
    )

    artifact.contract_validation_receipt = replace(receipt, schema_sha256="0" * 64)
    with pytest.raises(FileContractValidationError, match="schema_identity_mismatch"):
        require_file_contract_validation(artifact, _contract(), schema=schema)

    artifact.contract_validation_receipt = receipt
    with pytest.raises(FileContractValidationError, match="schema_identity_mismatch"):
        require_file_contract_validation(
            artifact,
            _contract(),
            schema=(("id", "bigint"), ("value", "nvarchar(max)")),
        )
