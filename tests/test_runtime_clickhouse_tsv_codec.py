from __future__ import annotations

from types import SimpleNamespace

from dpone.runtime.connectors.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse_external_artifact_source import ClickHouseExternalArtifactSource
from dpone.runtime.sinks.load_payload import LoadPayload


def test_codec_decodes_null_empty_escapes_and_reserved_prefix() -> None:
    codec = ClickHouseTabSeparatedCodec()

    assert codec.decode_wire_value(r"\N", source_type="nvarchar(max)") is None
    assert codec.decode_wire_value(codec.empty_string_marker, source_type="nvarchar(max)") == ""
    assert codec.decode_wire_value(r"tab\tline\nslash\\", source_type="nvarchar(max)") == "tab\tline\nslash\\"
    assert (
        codec.decode_wire_value(codec.escaped_prefix_marker + "value", source_type="nvarchar(max)")
        == codec.marker_prefix + "value"
    )


def test_external_artifact_seals_known_mssql_tsv_codec_losslessly(tmp_path) -> None:
    codec = ClickHouseTabSeparatedCodec()
    path = tmp_path / "rows.tsv"
    path.write_text(
        "\t".join((r"\N", codec.empty_string_marker, r"tab\tline\nslash\\", codec.escaped_prefix_marker + "x")) + "\n",
        encoding="utf-8",
    )
    columns = ("null_value", "empty_value", "escaped_value", "prefix_value")
    artifact = FileExportArtifact(
        str(path),
        columns,
        format="mssql-delimited",
        bulk_text_codec=codec,
        rows_exported=1,
    )
    schema = tuple((column, "nvarchar(max)") for column in columns)

    class Sink:
        _payload_ingestion = SimpleNamespace(
            _clickhouse_schema=lambda config, source_schema: tuple((name, "String") for name, _ in source_schema)
        )

    source = ClickHouseExternalArtifactSource(
        sink=Sink(),
        load_config=object(),
        payload=LoadPayload(artifact=artifact, schema=schema),
        maximum_rows=1,
    )

    assert source.open_replay().artifact._rows == [  # type: ignore[attr-defined]
        {
            "null_value": None,
            "empty_value": "",
            "escaped_value": "tab\tline\nslash\\",
            "prefix_value": codec.marker_prefix + "x",
        }
    ]
