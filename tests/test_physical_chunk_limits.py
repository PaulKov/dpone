from __future__ import annotations

from pathlib import Path

import pytest

from dpone.runtime.physical_chunking import PhysicalChunkLimitExceeded, PhysicalChunkPolicy, RowBoundaryChunkWriter
from dpone.runtime.source_scan import SourceScanPlanner, SourceShape


@pytest.mark.parametrize(
    ("target_chunk_bytes", "max_chunk_bytes", "error_code"),
    [
        (0, 32, "physical_chunk_target_bytes_invalid"),
        (-1, 32, "physical_chunk_target_bytes_invalid"),
        (8, 0, "physical_chunk_max_bytes_invalid"),
        (8, -1, "physical_chunk_max_bytes_invalid"),
        (33, 32, "physical_chunk_target_exceeds_max_bytes"),
    ],
)
def test_physical_chunk_policy_rejects_invalid_limits_before_bcp(
    target_chunk_bytes: int,
    max_chunk_bytes: int,
    error_code: str,
) -> None:
    with pytest.raises(ValueError, match=error_code):
        PhysicalChunkPolicy(target_chunk_bytes=target_chunk_bytes, max_chunk_bytes=max_chunk_bytes)


@pytest.mark.parametrize(
    ("field", "error_code"),
    [
        ("target_chunk_bytes", "physical_chunk_target_bytes_invalid"),
        ("max_chunk_bytes", "physical_chunk_max_bytes_invalid"),
    ],
)
@pytest.mark.parametrize("value", ["garbage", "0.1B", 1.5, "1 2MiB"])
def test_physical_chunk_policy_normalizes_invalid_values(field: str, error_code: str, value: object) -> None:
    physical_chunking = {"target_chunk_bytes": "64MiB", "max_chunk_bytes": "128MiB"}
    physical_chunking[field] = value

    with pytest.raises(ValueError, match=error_code):
        PhysicalChunkPolicy.from_source_options(
            {"native_transfer": {"snapshot": {"physical_chunking": physical_chunking}}}
        )


def test_physical_chunk_policy_uses_exact_decimal_math_for_hard_maximum() -> None:
    policy = PhysicalChunkPolicy.from_source_options(
        {
            "native_transfer": {
                "snapshot": {
                    "physical_chunking": {
                        "target_chunk_bytes": 1,
                        "max_chunk_bytes": "9007199254740995B",
                    }
                }
            }
        }
    )

    assert policy.max_chunk_bytes == 9_007_199_254_740_995


def test_source_scan_planner_uses_canonical_strict_physical_policy() -> None:
    with pytest.raises(ValueError, match="physical_chunk_target_bytes_invalid"):
        SourceScanPlanner().plan(
            source_options={
                "native_transfer": {
                    "snapshot": {
                        "physical_chunking": {
                            "target_chunk_bytes": "1 2MiB",
                            "max_chunk_bytes": "128MiB",
                        }
                    }
                }
            },
            source_shape=SourceShape(table_kind="heap"),
        )


def test_row_boundary_chunk_writer_seals_before_next_row_exceeds_max(tmp_path: Path) -> None:
    writer = _writer(tmp_path, target=8, maximum=10)

    chunks = list(writer.write([b"aaaaa\nbbbb\ncc\n"]))

    assert [Path(chunk.file_path).read_bytes() for chunk in chunks] == [b"aaaaa\n", b"bbbb\ncc\n"]
    assert [chunk.byte_count for chunk in chunks] == [6, 8]
    assert all(chunk.byte_count <= 10 for chunk in chunks)


def test_row_boundary_chunk_writer_accepts_exact_max_and_final_unterminated_row(tmp_path: Path) -> None:
    chunks = list(_writer(tmp_path, target=10, maximum=10).write([b"123456789\nfinal"]))

    assert [Path(chunk.file_path).read_bytes() for chunk in chunks] == [b"123456789\n", b"final"]
    assert [chunk.byte_count for chunk in chunks] == [10, 5]


def test_row_boundary_chunk_writer_rejects_oversized_complete_row_without_partial_file(tmp_path: Path) -> None:
    writer = _writer(tmp_path, target=8, maximum=8)

    with pytest.raises(PhysicalChunkLimitExceeded) as raised:
        list(writer.write([b"safe\n12345678\n"]))

    assert raised.value.chunk_index == 0
    assert raised.value.row_bytes == 9
    assert raised.value.max_chunk_bytes == 8
    assert "12345678" not in str(raised.value)
    assert not list(tmp_path.glob("dpone_physical_chunk_*.bcp"))


def test_row_boundary_chunk_writer_rejects_oversized_pending_row_before_eof(tmp_path: Path) -> None:
    with pytest.raises(PhysicalChunkLimitExceeded, match="physical_chunk_row_exceeds_max_bytes"):
        list(_writer(tmp_path, target=8, maximum=8).write([b"1234", b"56789"]))

    assert not list(tmp_path.glob("dpone_physical_chunk_*.bcp"))


def test_row_boundary_chunk_writer_empty_input_emits_nothing(tmp_path: Path) -> None:
    assert list(_writer(tmp_path, target=8, maximum=8).write([])) == []
    assert not list(tmp_path.glob("dpone_physical_chunk_*.bcp"))


def test_row_boundary_chunk_writer_does_not_precreate_unconsumed_chunk(tmp_path: Path) -> None:
    chunks = _writer(tmp_path, target=8, maximum=10).write([b"aaaaa\n12345678\n"])

    first = next(chunks)
    chunks.close()

    assert [path.resolve() for path in tmp_path.glob("dpone_physical_chunk_*.bcp")] == [Path(first.file_path).resolve()]
    first.cleanup()


def _writer(tmp_path: Path, *, target: int, maximum: int) -> RowBoundaryChunkWriter:
    return RowBoundaryChunkWriter(
        policy=PhysicalChunkPolicy(target_chunk_bytes=target, max_chunk_bytes=maximum),
        columns=("value",),
        directory=tmp_path,
        format="mssql-delimited",
    )
