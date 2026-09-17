"""Execute transport macros with dbt's actual Jinja and statement primitives."""

import re
from numbers import Number
from pathlib import Path
from types import SimpleNamespace

import pytest

agate = pytest.importorskip("agate")
BaseContext = pytest.importorskip("dbt.context.base").BaseContext
dbt_jinja = pytest.importorskip("dbt_common.clients.jinja")
BaseMacroGenerator, get_template = dbt_jinja.BaseMacroGenerator, dbt_jinja.get_template

ROOT = Path(__file__).resolve().parents[1]
MACROS = ROOT / "packages/dbt-dpone/macros/physical"


class Generator(BaseMacroGenerator):
    def __init__(self, name, template, context):
        super().__init__(context)
        self.name, self.template = name, template

    def get_name(self):
        return self.name

    def get_template(self):
        return self.template

    def __call__(self, *args, **kwargs):
        return self.call_macro(*args, **kwargs)


def harness(*, execute=True, envelope=None, response=None):
    from dbt.include import global_project

    statements = Path(global_project.PACKAGE_PATH) / "macros/etc/statement.sql"
    sources = [statements.read_text()] + [
        (MACROS / name).read_text() for name in ("managed_wire.sql", "mssql_admission.sql")
    ]
    calls, results = [], {}

    def fail(message):
        raise ValueError(message)

    def adapter_execute(sql, *, auto_begin, fetch):
        calls.append((sql.strip(), auto_begin, fetch))
        return {}, response

    def store_result(name, *, response, agate_table):
        results[name] = SimpleNamespace(table=agate_table)
        return ""

    context = {
        "execute": execute,
        "return": BaseContext._return,
        "fromjson": BaseContext.fromjson,
        "modules": {"re": re},
        "exceptions": SimpleNamespace(raise_compiler_error=fail),
        "var": lambda name: envelope,
        "adapter": SimpleNamespace(execute=adapter_execute),
        "store_result": store_result,
        "load_result": results.__getitem__,
    }
    for source in sources:
        template = get_template(source, context)
        for name in re.findall(r"{%[- ]*macro\s+(\w+)", source):
            context[name] = Generator(name, template, context)
    return context, calls


@pytest.mark.parametrize(
    "name,args",
    [
        ("dpone_mssql_attach", ("model.project.orders",)),
        ("dpone_mssql_bind_transaction", (None,)),
        ("dpone_mssql_require_transaction", (None, None)),
    ],
)
def test_parse_has_no_database_work(name, args):
    context, calls = harness(execute=False)
    assert context[name](*args) is None
    assert calls == []


UUID = "12345678-1234-1234-1234-123456789abc"
DIGEST = "sha256:" + "a" * 64
REF = {"locator": "retained/original.json", "sha256": DIGEST}
ENVELOPE = {
    "generation_id": UUID,
    "invocation_id": UUID,
    "runtime_registration_id": UUID,
    "plan_set": REF,
}


def observation():
    from dpone.contracts.dbt_mssql_physical import (
        AbsentPredecessor,
        PhysicalFilegroup,
        PhysicalModelPlan,
        PhysicalModelSpec,
        PhysicalRelation,
    )
    from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
    from dpone.contracts.native_identity import OriginalRef
    from dpone.contracts.strict_json import canonical_json_bytes

    plan = PhysicalModelPlan(
        UUID,
        PhysicalModelSpec(
            "model.project.orders",
            DIGEST,
            PhysicalRelation("db", "dbo", "orders"),
            (MssqlCatalogColumn("id", "int", False, None),),
            "rowstore_none",
            PhysicalFilegroup(1, "PRIMARY"),
            OriginalRef(**REF),
        ),
        AbsentPredecessor(),
    ).to_dict()
    executor = {
        "schema": "dpone.native-source-executor-binding.v1",
        "generation_id": UUID,
        "guard_epoch": 1,
        "invocation_id": UUID,
        "reservation": REF,
        "profile": REF,
        "command": REF,
    }
    return dict(
        zip(
            [
                "wire_version",
                "registration_id",
                "registration_digest",
                "generation_id",
                "executor_invocation_id",
                "plan_set_sha256",
                "model_unique_id",
                "model_plan_sha256",
                "session_registration_id",
                "session_id",
                "guard_epoch",
                "source_revision",
                "control_program_sha256",
                "executor_json",
                "plan_json",
            ],
            [
                1,
                UUID,
                DIGEST,
                UUID,
                UUID,
                DIGEST,
                "model.project.orders",
                plan["model_plan_sha256"],
                UUID,
                37,
                1,
                2,
                DIGEST,
                canonical_json_bytes(executor).decode(),
                canonical_json_bytes(plan).decode(),
            ],
            strict=True,
        )
    )


def table(row):
    # Explicit types match a SQL driver/agate transport; no Boolean inference of 1.
    types = [agate.Number() if isinstance(value, Number) else agate.Text() for value in row.values()]
    return agate.Table([list(row.values())], list(row), types)


def transaction(attach):
    names = [
        "wire_version",
        "registration_id",
        "registration_digest",
        "generation_id",
        "executor_invocation_id",
        "model_unique_id",
        "model_plan_sha256",
        "session_registration_id",
        "session_id",
    ]
    return {
        **{name: attach[name] for name in names},
        "transaction_id": 987,
        "transaction_count": 1,
        "transaction_state": 1,
    }


def test_real_statement_attach_bind_require():
    row = observation()
    context, calls = harness(envelope=ENVELOPE, response=table(row))
    attached = context["dpone_mssql_attach"]("model.project.orders")
    assert attached == row
    assert len(calls) == 1 and calls[0][1:] == (False, True)
    assert "EXEC [dpone_physical].[physical_attach_session_v1]" in calls[0][0]
    assert "@plan_set_locator=N'retained/original.json'" in calls[0][0]
    bound = transaction(attached)
    context, calls = harness(response=table(bound))
    assert context["dpone_mssql_bind_transaction"](attached) == bound
    assert "physical_bind_transaction_v1" in calls[0][0]
    assert context["dpone_mssql_require_transaction"](attached, bound) == bound
    assert "@expected_transaction_id=987" in calls[1][0]
    assert all(call[1:] == (False, True) for call in calls)


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {**ENVELOPE, "extra": 1},
        {**ENVELOPE, "generation_id": UUID.upper()},
        {**ENVELOPE, "invocation_id": UUID + "';DROP TABLE x;--"},
        {**ENVELOPE, "plan_set": {**REF, "extra": 1}},
        {**ENVELOPE, "plan_set": {**REF, "locator": "../x"}},
        {**ENVELOPE, "plan_set": {**REF, "sha256": DIGEST + "x"}},
        {**ENVELOPE, "plan_set": {**REF, "locator": "😀" * 1025}},
        {**ENVELOPE, "plan_set": {**REF, "locator": "a\ud800"}},
    ],
)
def test_bad_envelope_never_queries(value):
    context, calls = harness(envelope=value)
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_mssql_attach"]("model.project.orders")
    assert not calls


def test_unicode_literal_escapes_quotes_without_changing_identity():
    context, _ = harness()
    assert context["dpone_managed_literal"]("a'雪;--") == "N'a''雪;--'"


@pytest.mark.parametrize(
    "field,value",
    [
        ("wire_version", 2),
        ("wire_version", True),
        ("session_id", 0),
        ("guard_epoch", 2**63),
        ("source_revision", 1),
        ("registration_digest", DIGEST.upper()),
        ("registration_id", None),
        ("plan_set_sha256", "sha256:" + "b" * 64),
        ("model_unique_id", "different"),
        ("executor_invocation_id", "0" * 36),
    ],
)
def test_attach_rejects_scalar_corruption(field, value):
    row = observation()
    row[field] = value
    # Retain raw bool/NULL to test strict acceptance instead of agate coercion.
    response = SimpleNamespace(column_names=list(row), rows=[list(row.values())])
    context, _ = harness(envelope=ENVELOPE, response=response)
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_mssql_attach"]("model.project.orders")


@pytest.mark.parametrize("mode", ["missing", "extra", "order", "zero", "two", "none"])
def test_exact_result_shape(mode):
    row = observation()
    names, rows = list(row), [list(row.values())]
    if mode == "missing":
        names.pop()
    if mode == "extra":
        names.append("unexpected")
    if mode == "order":
        names.reverse()
    if mode == "zero":
        rows = []
    if mode == "two":
        rows *= 2
    response = None if mode == "none" else SimpleNamespace(column_names=names, rows=rows)
    context, _ = harness(envelope=ENVELOPE, response=response)
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_mssql_attach"]("model.project.orders")


@pytest.mark.parametrize(
    "raw",
    [
        '{"a":1,"a":2}',
        '{"a":{"b":1,"b":2}}',
        '{"a":1,"\\u0061":2}',
        '{"a":NaN}',
        '{"a":1.5}',
        '{"a":1e2}',
        '{"a":' + "1" * 129 + "}",
        '{"a":"\\ud800"}',
        '{"a":"' + "😀" * 1025 + '"}',
        '{"a":' + "[" * 32 + "0" + "]" * 32 + "}",
        '{"a":[' + "0," * 33000 + "0]}",
        '{"a":1} garbage',
        "{}" * 524289,
        '{"a":}',
        '{"a":"unterminated}',
    ],
)
def test_bounded_duplicate_free_json(raw):
    context, _ = harness()
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_managed_json"](raw, "test")


@pytest.mark.parametrize(
    "path,value",
    [
        (("executor_json", "extra"), 1),
        (("executor_json", "guard_epoch"), True),
        (("executor_json", "generation_id"), "other"),
        (("executor_json", "command", "sha256"), "bad"),
        (("plan_json", "schema"), "future"),
        (("plan_json", "model_plan_sha256"), DIGEST),
        (("plan_json", "spec", "model_unique_id"), "other"),
        (("plan_json", "spec", "relation", "table"), "x" * 129),
        (("plan_json", "spec", "layout"), "heap"),
        (("plan_json", "spec", "columns"), []),
        (("plan_json", "spec", "filegroup", "data_space_id"), True),
        (("plan_json", "predecessor", "kind"), "legacy"),
        (("plan_json", "backup_name"), "unrequested"),
        (("plan_json", "columnstore_index_name"), "unrequested"),
    ],
)
def test_nested_shape_and_cross_identity(path, value):
    import json

    row = observation()
    payload = json.loads(row[path[0]])
    target = payload
    for key in path[1:-1]:
        target = target[key]
    target[path[-1]] = value
    row[path[0]] = json.dumps(payload)
    context, _ = harness(envelope=ENVELOPE, response=table(row))
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_mssql_attach"]("model.project.orders")


@pytest.mark.parametrize(
    "field",
    [
        "registration_id",
        "registration_digest",
        "generation_id",
        "executor_invocation_id",
        "model_unique_id",
        "model_plan_sha256",
        "session_registration_id",
        "session_id",
        "transaction_id",
        "transaction_count",
        "transaction_state",
    ],
)
def test_require_rejects_changed_transaction_observation(field):
    row = observation()
    bound = transaction(row)
    changed = dict(bound)
    value = changed[field]
    changed[field] = value + 1 if isinstance(value, int) else value.replace("a", "b") + "x"
    context, calls = harness(response=table(changed))
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_mssql_require_transaction"](row, bound)
    assert len(calls) == 1


def test_require_rejects_invalid_input_before_query():
    row = observation()
    bound = {**transaction(row), "transaction_id": "1; COMMIT;--"}
    context, calls = harness()
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_mssql_require_transaction"](row, bound)
    assert not calls


def test_driver_error_propagates_without_retry():
    context, calls = harness(envelope=ENVELOPE)

    def fail(sql, **kwargs):
        calls.append(sql)
        raise ConnectionError("lost acknowledgement")

    context["adapter"].execute = fail
    with pytest.raises(ConnectionError, match="lost acknowledgement"):
        context["dpone_mssql_attach"]("model.project.orders")
    assert len(calls) == 1


@pytest.mark.parametrize("value", [1.0, "1", -1, 0, 2**63, float("nan"), float("inf"), True])
def test_transaction_integer_lexical_contract(value):
    context, _ = harness()
    with pytest.raises(ValueError, match="DPONE_MANAGED_INPUT_INVALID"):
        context["dpone_managed_integer"](value, "number")
