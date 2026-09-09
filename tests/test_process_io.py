from __future__ import annotations

from io import BytesIO, StringIO
from types import SimpleNamespace

from dpone.runtime.process_io import ProcessOutputDrainer


def test_process_output_drainer_decodes_binary_pipes() -> None:
    seen: list[str] = []
    process = SimpleNamespace(
        stdout=BytesIO(b"ok\n"),
        stderr=BytesIO("Ошибка ClickHouse\n".encode()),
    )

    drainer = ProcessOutputDrainer(process, stderr_callback=seen.append)
    drainer.start()

    capture = drainer.join()

    assert capture.stdout == "ok\n"
    assert capture.stderr == "Ошибка ClickHouse\n"
    assert seen == ["Ошибка ClickHouse"]


def test_process_output_drainer_keeps_text_pipes() -> None:
    process = SimpleNamespace(stdout=StringIO("ok\n"), stderr=StringIO("warn\n"))

    drainer = ProcessOutputDrainer(process)
    drainer.start()

    capture = drainer.join()

    assert capture.stdout == "ok\n"
    assert capture.stderr == "warn\n"
