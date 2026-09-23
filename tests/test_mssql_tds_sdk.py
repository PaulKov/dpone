"""The SDK result cannot replace independent input completion authority."""

from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_tds_sdk as sdk
from dpone.contracts.mssql_native_chunks import TdsInputReceipt

H = "a" * 64


class Input:
    def __init__(self, rows=2, fail=False):
        self.rows, self.fail, self.finished = rows, fail, False

    def iter_rows(self):
        for i in range(self.rows):
            yield (i,)
        if self.fail:
            raise ValueError("sensitive input detail")
        self.finished = True

    iter_arrow_batches = iter_rows

    def require_complete(self):
        if not self.finished:
            raise ValueError("sensitive incomplete detail")
        return TdsInputReceipt(self.rows, self.rows * 8, H)


class Cursor:
    def __init__(self, response=None, swallow=False):
        self.response = {"rows_copied": 2} if response is None else response
        self.swallow, self.calls, self.closed = swallow, [], False

    def bulkcopy(self, target, data, **kwargs):
        self.calls.append((target, kwargs))
        try:
            list(data)
        except ValueError:
            if not self.swallow:
                raise
        return self.response

    bulkcopy_arrow = bulkcopy

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def admitted(monkeypatch):
    monkeypatch.setattr(sdk, "admit_tds_sdk", lambda **kwargs: None)


def copy(cursor, source=None, **kwargs):
    source = source or Input()
    options = dict(
        input_mode="rows", columns=("v",), expected=TdsInputReceipt(2, 16, H), batch_rows=2, timeout_seconds=30
    )
    options.update(kwargs)
    return sdk.copy_owned_stage(SimpleNamespace(cursor=lambda: cursor), "[test].[owned]", source, **options)


@pytest.mark.parametrize("mode", ["rows", "arrow"])
def test_explicit_options_and_independent_receipt(mode):
    cursor = Cursor()
    assert copy(cursor, input_mode=mode) == TdsInputReceipt(2, 16, H)
    assert cursor.calls == [
        (
            "[test].[owned]",
            dict(
                batch_size=2,
                timeout=30,
                column_mappings=["v"],
                keep_identity=False,
                check_constraints=True,
                table_lock=True,
                keep_nulls=True,
                fire_triggers=True,
                use_internal_transaction=True,
            ),
        )
    ]
    assert cursor.closed


@pytest.mark.parametrize(
    "response", [{}, {"rows_copied": True}, {"rows_copied": 1}, {"rows_copied": "2"}, [], {"rows_copied": -1}]
)
def test_malformed_or_wrong_sdk_result_rejected(response):
    with pytest.raises(ValueError, match="tds_sdk_result_mismatch"):
        copy(Cursor(response=response))


def test_swallowed_iterator_error_fails_completion():
    with pytest.raises(ValueError, match="tds_input_incomplete"):
        copy(Cursor(swallow=True), Input(fail=True))


@pytest.mark.parametrize("mode", ["rows", "arrow"])
def test_empty_input_verified_without_sql(mode):
    cursor = Cursor()
    assert copy(cursor, Input(0), input_mode=mode, expected=TdsInputReceipt(0, 0, H)).rows == 0
    assert not cursor.calls and not cursor.closed


@pytest.mark.parametrize("expected", [TdsInputReceipt(2, 17, H), TdsInputReceipt(2, 16, "b" * 64)])
def test_complete_input_must_match_expected_identity(expected):
    with pytest.raises(ValueError, match="tds_input_identity_mismatch"):
        copy(Cursor(), expected=expected)


@pytest.mark.parametrize(
    "option,value",
    [
        ("batch_rows", True),
        ("batch_rows", 0),
        ("batch_rows", 65537),
        ("timeout_seconds", True),
        ("timeout_seconds", 0),
        ("timeout_seconds", 3601),
        ("columns", ("v", "v")),
        ("columns", ("",)),
        ("columns", ["v"]),
        ("expected", {}),
    ],
)
def test_invalid_options_fail_before_copy(option, value):
    cursor = Cursor()
    with pytest.raises(ValueError):
        copy(cursor, **{option: value})
    assert not cursor.calls


def test_empty_iterator_failure_is_sanitized():
    with pytest.raises(ValueError, match="tds_input_incomplete") as caught:
        copy(Cursor(), Input(0, fail=True), expected=TdsInputReceipt(0, 0, H))
    import traceback

    assert "sensitive" not in "".join(traceback.format_exception(caught.value))


def test_driver_error_sanitized_and_cursor_closed():
    cursor = Cursor()
    with pytest.raises(ValueError, match="tds_sdk_copy_failed") as caught:
        copy(cursor, Input(fail=True))
    import traceback

    assert "sensitive" not in "".join(traceback.format_exception(caught.value))
    assert cursor.closed


def test_cleanup_error_invalidates_success():
    cursor = Cursor()
    cursor.close = lambda: (_ for _ in ()).throw(RuntimeError("sensitive cleanup"))
    with pytest.raises(ValueError, match="tds_sdk_cleanup_failed"):
        copy(cursor)


def test_cancellation_preserved_even_when_cursor_close_fails():
    cursor = Cursor()

    def cancel(*args, **kwargs):
        raise KeyboardInterrupt

    cursor.bulkcopy = cancel
    cursor.close = lambda: (_ for _ in ()).throw(RuntimeError("sensitive cleanup"))
    with pytest.raises(KeyboardInterrupt):
        copy(cursor)


@pytest.mark.parametrize("primary_failure", [False, True])
@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_input_close_interrupt_still_closes_cursor_and_preserves_primary(primary_failure, interrupt):
    class InterruptingInput(Input):
        def iter_rows(self):
            original = super().iter_rows()

            class Rows:
                def __iter__(self):
                    return self

                def __next__(self):
                    return next(original)

                def close(self):
                    original.close()
                    raise interrupt

            return Rows()

    cursor = Cursor()
    expected_error = ValueError if primary_failure else interrupt
    with pytest.raises(expected_error) as caught:
        copy(cursor, InterruptingInput(fail=primary_failure))
    if primary_failure:
        assert str(caught.value) == "mssql_native.tds_sdk_copy_failed"
    assert cursor.closed


def test_partial_sdk_consumption_cannot_succeed():
    cursor = Cursor()
    cursor.bulkcopy = lambda *args, **kwargs: {"rows_copied": 2}
    with pytest.raises(ValueError, match="tds_input_incomplete"):
        copy(cursor)


@pytest.mark.parametrize("mode", ["rows", "arrow"])
def test_actual_native_input_roundtrip(tmp_path, mode):
    pytest.importorskip("pyarrow")
    from dpone.adapters.mssql_tds_input import NativeTdsInput
    from dpone.runtime.mssql_native_chunks_files import encode_native_frame
    from dpone.runtime.mssql_tds_decoder import MssqlTdsRowDecoder
    from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract

    contract = build_mssql_bcp_native_contract(schema=[("v", "bigint")], query="synthetic")
    file = encode_native_frame(contract, ({"v": -(2**63)}, {"v": 2**63 - 1}), tmp_path / "chunk.native", 0, 1024, 2048)
    expected = TdsInputReceipt(file.rows, file.encoded_bytes, file.file_sha256)
    source = NativeTdsInput(
        file, MssqlTdsRowDecoder(contract, input_mode=mode), input_mode=mode, batch_rows=1, max_row_bytes=1024
    )
    assert copy(Cursor(), source, input_mode=mode, expected=expected) == expected


_REAL_ADMISSION = sdk.admit_tds_sdk


@pytest.mark.parametrize("mode", ["rows", "arrow"])
@pytest.mark.parametrize(
    "fault",
    [None, "driver_version", "arrow_version", "missing_package", "missing_method", "missing_core", "missing_arrow"],
)
def test_exact_lazy_dependency_admission(monkeypatch, mode, fault):
    from importlib import metadata

    def version(name):
        if fault == "missing_package":
            raise metadata.PackageNotFoundError
        if name == "mssql-python":
            return "1.12.0" if fault == "driver_version" else "1.13.0"
        return "24.0.0" if fault == "arrow_version" else "25.0.1"

    def load(name):
        if fault == "missing_core" and name == "mssql_py_core":
            raise ImportError("sensitive module error")
        if fault == "missing_arrow" and name == "pyarrow":
            raise ImportError
        methods = SimpleNamespace(
            bulkcopy=lambda: None, bulkcopy_arrow=None if fault == "missing_method" else lambda: None
        )
        if name == "mssql_python.cursor":
            return SimpleNamespace(Cursor=methods)
        if name == "mssql_py_core":
            return SimpleNamespace(PyCoreCursor=methods, PyCoreConnection=lambda: None)
        return SimpleNamespace(array=lambda: None, RecordBatch=lambda: None)

    monkeypatch.setattr(sdk.metadata, "version", version)
    monkeypatch.setattr(sdk.importlib, "import_module", load)
    fails = fault in {"driver_version", "missing_package", "missing_core"} or (
        mode == "arrow" and fault in {"arrow_version", "missing_method", "missing_arrow"}
    )
    if fails:
        with pytest.raises(ValueError, match="unavailable_or_unadmitted"):
            _REAL_ADMISSION(input_mode=mode)
    else:
        _REAL_ADMISSION(input_mode=mode)


def test_early_sdk_return_closes_started_input_generator():
    class TrackedInput(Input):
        closed = False

        def iter_rows(self):
            try:
                yield from super().iter_rows()
            finally:
                self.closed = True

    source = TrackedInput()
    cursor = Cursor()

    def stop_early(target, data, **kwargs):
        next(data)
        return {"rows_copied": 2}

    cursor.bulkcopy = stop_early
    with pytest.raises(ValueError, match="tds_input_incomplete"):
        copy(cursor, source)
    assert source.closed and cursor.closed


def test_admission_failure_precedes_source_and_cursor(monkeypatch):
    def unavailable(**kwargs):
        raise ValueError("mssql_native.tds_sdk_unavailable_or_unadmitted")

    monkeypatch.setattr(sdk, "admit_tds_sdk", unavailable)
    source = Input()
    cursor = Cursor()
    with pytest.raises(ValueError, match="unavailable_or_unadmitted"):
        copy(cursor, source)
    assert not cursor.calls and not cursor.closed and not source.finished
