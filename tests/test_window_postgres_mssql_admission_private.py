"""The window branch must preserve prebound route authority instead of bypassing it."""

from types import SimpleNamespace

import pytest

from dpone.contracts.process_errors import ETLProcessError
from dpone.runtime.bootstrap_runner import DefaultProcessRunner


@pytest.mark.parametrize("field", ["postgres_mssql_correctness_activation", "postgres_mssql_correctness_runtime"])
@pytest.mark.parametrize("value", [object(), False, {}, ""])
def test_prebound_correctness_is_rejected_before_factory_or_hydration(field, value):
    calls = []

    def factory(_):
        calls.append("factory")
        return SimpleNamespace(run=lambda *a, **kw: SimpleNamespace(status="success", errors=[]))

    config = SimpleNamespace(
        name="window",
        load_config=SimpleNamespace(options={"rolling_window": {}}),
        ensure_runtime_bindings=lambda: calls.append("hydrate"),
        **{field: value},
    )
    with pytest.raises(ETLProcessError, match="rolling_window_unsupported_policy: postgres_mssql_correctness"):
        DefaultProcessRunner(window_runtime_factory=factory).run(SimpleNamespace(config=config))
    assert calls == []


@pytest.mark.parametrize(
    "fields", [{}, {"postgres_mssql_correctness_activation": None, "postgres_mssql_correctness_runtime": None}]
)
def test_unbound_window_retains_new_master_execution(fields):
    calls = []
    result = SimpleNamespace(status="success", errors=[])

    def factory(_):
        calls.append("factory")
        return SimpleNamespace(run=lambda *a, **kw: result)

    config = SimpleNamespace(
        name="window",
        load_config=SimpleNamespace(options={"rolling_window": {}}),
        ensure_runtime_bindings=lambda: pytest.fail("window must not hydrate legacy runtime"),
        **fields,
    )
    process = SimpleNamespace(config=config)
    assert DefaultProcessRunner(window_runtime_factory=factory).run(process) is result
    assert process.current_state is result
    assert calls == ["factory"]
