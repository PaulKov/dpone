from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import RuntimeExecutionSelection
from dpone.runtime.verified_pack_hook_policy import verified_runtime_environment
from dpone.runtime.verified_pack_launcher import _selected_command

_LEGACY_EXPANDED_ENV = {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"}


def _execution(
    *,
    selector: str = "orders",
    scope: str | None = None,
    process_selector: str | None = None,
    hook_execution: str | None = None,
) -> RuntimeExecutionSelection:
    return RuntimeExecutionSelection(
        kind="runtime",
        selector=selector,
        scope=scope,
        process_selector=process_selector,
        hook_execution=hook_execution,
    )


def _pack(visibility: str) -> dict[str, object]:
    return {
        "process_plans": {
            "__default__": {
                "selector": None,
                "dag_node": {
                    "visibility": visibility,
                },
            }
        }
    }


def test_inline_process_plan_executes_hooks_inside_strict_runtime_task() -> None:
    environment = verified_runtime_environment(
        _pack("inline"),
        execution=_execution(),
        value=_LEGACY_EXPANDED_ENV,
    )

    assert environment == {}


def test_verified_launcher_applies_inline_hook_policy() -> None:
    pack = {
        **_pack("inline"),
        "runtime_bootstrap": {
            "schema": "dpone.airflow-runtime-bootstrap.v1",
            "commands": {
                "__default__": {
                    "argv": [
                        "dpone",
                        "run",
                        "runtime/orders.yaml",
                        "--format",
                        "json",
                    ],
                    "env": _LEGACY_EXPANDED_ENV,
                }
            },
        },
    }
    plan = SimpleNamespace(execution=_execution())

    argv, environment = _selected_command(pack, plan)

    assert argv[:3] == ("dpone", "run", "runtime/orders.yaml")
    assert environment == {}


def test_legacy_runtime_bridge_rejects_selected_multi_process_pack() -> None:
    pack = {
        "runtime_bootstrap": {
            "schema": "dpone.airflow-runtime-bootstrap.v1",
            "commands": {
                "orders": {
                    "argv": [
                        "dpone",
                        "run",
                        "runtime/orders.yaml",
                        "--format",
                        "json",
                        "--selector",
                        "orders",
                    ],
                    "env": _LEGACY_EXPANDED_ENV,
                },
                "customers": {
                    "argv": [
                        "dpone",
                        "run",
                        "runtime/orders.yaml",
                        "--format",
                        "json",
                        "--selector",
                        "customers",
                    ],
                    "env": _LEGACY_EXPANDED_ENV,
                },
            },
        },
        "process_plans": {
            "orders": {
                "selector": "orders",
                "dag_node": {"visibility": "inline"},
            },
            "customers": {
                "selector": "customers",
                "dag_node": {"visibility": "inline"},
            },
        },
    }
    plan = SimpleNamespace(execution=_execution(selector="orders"))

    with pytest.raises(InitFetchError) as exc_info:
        _selected_command(pack, plan)

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_legacy_runtime_bridge_derives_single_process_command_from_pack() -> None:
    pack = {
        "runtime_bootstrap": {
            "schema": "dpone.airflow-runtime-bootstrap.v1",
            "commands": {
                "orders": {
                    "argv": [
                        "dpone",
                        "run",
                        "runtime/orders.yaml",
                        "--format",
                        "json",
                        "--selector",
                        "orders",
                    ],
                    "env": _LEGACY_EXPANDED_ENV,
                },
                "customers": {
                    "argv": [
                        "dpone",
                        "run",
                        "runtime/orders.yaml",
                        "--format",
                        "json",
                        "--selector",
                        "customers",
                    ],
                    "env": _LEGACY_EXPANDED_ENV,
                },
            },
        },
        "process_plans": {
            "customers": {
                "selector": "customers",
                "dag_node": {"visibility": "inline"},
            }
        },
    }
    plan = SimpleNamespace(execution=_execution(selector="orders"))

    argv, environment = _selected_command(pack, plan)

    assert argv[-2:] == ("--selector", "customers")
    assert environment == {}


@pytest.mark.parametrize("visibility", ("task", "group"))
def test_expanded_process_plan_keeps_separate_hook_skip(
    visibility: str,
) -> None:
    environment = verified_runtime_environment(
        _pack(visibility),
        execution=_execution(),
        value=_LEGACY_EXPANDED_ENV,
    )

    assert environment == _LEGACY_EXPANDED_ENV


def test_legacy_pack_without_process_plan_fails_closed() -> None:
    with pytest.raises(InitFetchError) as exc_info:
        verified_runtime_environment(
            {},
            execution=_execution(),
            value=_LEGACY_EXPANDED_ENV,
        )

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_single_named_process_plan_is_unambiguous_for_legacy_plan() -> None:
    environment = verified_runtime_environment(
        {
            "process_plans": {
                "orders": {
                    "selector": "orders",
                    "dag_node": {"visibility": "inline"},
                }
            }
        },
        execution=_execution(selector="batch"),
        value=_LEGACY_EXPANDED_ENV,
    )

    assert environment == {}


def test_unselected_multi_process_pack_fails_closed() -> None:
    with pytest.raises(InitFetchError) as exc_info:
        verified_runtime_environment(
            {
                "process_plans": {
                    "orders": {
                        "selector": "orders",
                        "dag_node": {"visibility": "inline"},
                    },
                    "customers": {
                        "selector": "customers",
                        "dag_node": {"visibility": "inline"},
                    },
                }
            },
            execution=_execution(selector="batch"),
            value=_LEGACY_EXPANDED_ENV,
        )

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.parametrize(
    "pack",
    (
        {"process_plans": []},
        {"process_plans": {"__default__": []}},
        {"process_plans": {"__default__": {}}},
        {
            "process_plans": {
                "__default__": {
                    "selector": None,
                    "dag_node": {"visibility": "hidden"},
                }
            }
        },
    ),
)
def test_malformed_verified_process_plan_fails_closed(
    pack: dict[str, object],
) -> None:
    with pytest.raises(InitFetchError) as exc_info:
        verified_runtime_environment(
            pack,
            execution=_execution(),
            value=_LEGACY_EXPANDED_ENV,
        )

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.parametrize(
    ("plans", "selector"),
    (
        (
            {
                "orders": {
                    "selector": "customers",
                    "dag_node": {"visibility": "inline"},
                }
            },
            "orders",
        ),
        (
            {
                "__default__": {
                    "selector": "orders",
                    "dag_node": {"visibility": "inline"},
                }
            },
            "batch",
        ),
        (
            {
                "orders": {
                    "selector": None,
                    "dag_node": {"visibility": "inline"},
                }
            },
            "batch",
        ),
    ),
)
def test_legacy_process_plan_selector_identity_mismatch_fails_closed(
    plans: dict[str, object],
    selector: str,
) -> None:
    with pytest.raises(InitFetchError) as exc_info:
        verified_runtime_environment(
            {"process_plans": plans},
            execution=_execution(selector=selector),
            value=_LEGACY_EXPANDED_ENV,
        )

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


@pytest.mark.parametrize("hook_execution, expected", (("inline", {}), ("externalized", _LEGACY_EXPANDED_ENV)))
def test_explicit_hook_execution_uses_exact_process_selector(
    hook_execution: str,
    expected: dict[str, str],
) -> None:
    pack = {
        "process_plans": {
            "dbo.orders": {
                "selector": "dbo.orders",
                "dag_node": {"visibility": "inline"},
            }
        }
    }

    environment = verified_runtime_environment(
        pack,
        execution=_execution(
            selector="orders",
            scope="process",
            process_selector="dbo.orders",
            hook_execution=hook_execution,
        ),
        value=_LEGACY_EXPANDED_ENV,
    )

    assert environment == expected


def test_explicit_hook_execution_rejects_process_selector_mismatch() -> None:
    with pytest.raises(InitFetchError) as exc_info:
        verified_runtime_environment(
            {
                "process_plans": {
                    "dbo.customers": {
                        "selector": "dbo.customers",
                        "dag_node": {"visibility": "inline"},
                    }
                }
            },
            execution=_execution(
                process_selector="dbo.orders",
                scope="process",
                hook_execution="inline",
            ),
            value=_LEGACY_EXPANDED_ENV,
        )

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_explicit_process_selector_requires_process_plan_inventory() -> None:
    with pytest.raises(InitFetchError) as exc_info:
        verified_runtime_environment(
            {},
            execution=_execution(
                process_selector="dbo.orders",
                scope="process",
                hook_execution="inline",
            ),
            value=_LEGACY_EXPANDED_ENV,
        )

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_explicit_whole_workload_execution_allows_multiple_process_plans() -> None:
    environment = verified_runtime_environment(
        {
            "process_plans": {
                "dbo.orders": {"selector": "dbo.orders"},
                "dbo.customers": {"selector": "dbo.customers"},
            }
        },
        execution=_execution(
            scope="workload",
            hook_execution="externalized",
        ),
        value=_LEGACY_EXPANDED_ENV,
    )

    assert environment == _LEGACY_EXPANDED_ENV


def test_legacy_generated_hook_bridge_rejects_ambiguous_process_commands() -> None:
    def step(selector: str) -> dict[str, object]:
        argv = [
            "dpone",
            "hooks",
            "execute",
            "runtime/orders.yaml",
            "--phase",
            "pre_hook",
            "--hook-id",
            "refresh_orders",
            "--selector",
            selector,
        ]
        return {
            "name": "pre_hook_refresh_orders",
            "phase": "pre_hook",
            "command": " ".join(argv),
            "runtime_command": {
                "schema": "dpone.airflow-pre-hook-command.v1",
                "hook_id": "refresh_orders",
                "process_selector": selector,
                "argv": argv,
            },
        }

    pack = {
        "steps": [],
        "process_plans": {
            "dbo.orders": {
                "selector": "dbo.orders",
                "steps": [step("dbo.orders")],
            },
            "dbo.customers": {
                "selector": "dbo.customers",
                "steps": [step("dbo.customers")],
            },
        },
    }
    plan = SimpleNamespace(
        execution=RuntimeExecutionSelection(
            kind="pre_hook",
            selector="orders",
            hook_name="pre_hook_refresh_orders",
        )
    )

    with pytest.raises(InitFetchError) as exc_info:
        _selected_command(pack, plan)

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_legacy_generated_hook_bridge_rejects_command_argv_mismatch() -> None:
    structured_argv = [
        "dpone",
        "hooks",
        "execute",
        "runtime/orders.yaml",
        "--phase",
        "pre_hook",
        "--hook-id",
        "refresh_orders",
        "--selector",
        "dbo.orders",
    ]
    pack = {
        "steps": [],
        "process_plans": {
            "dbo.orders": {
                "selector": "dbo.orders",
                "steps": [
                    {
                        "name": "pre_hook_refresh_orders",
                        "phase": "pre_hook",
                        "command": (
                            "dpone hooks execute runtime/orders.yaml --phase pre_hook "
                            "--hook-id refresh_customers --selector dbo.orders"
                        ),
                        "runtime_command": {
                            "schema": "dpone.airflow-pre-hook-command.v1",
                            "hook_id": "refresh_orders",
                            "process_selector": "dbo.orders",
                            "argv": structured_argv,
                        },
                    }
                ],
            }
        },
    }
    plan = SimpleNamespace(
        execution=RuntimeExecutionSelection(
            kind="pre_hook",
            selector="orders",
            hook_name="pre_hook_refresh_orders",
        )
    )

    with pytest.raises(InitFetchError) as exc_info:
        _selected_command(pack, plan)

    assert exc_info.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_v3_default_process_pre_hook_does_not_select_same_named_workload_hook() -> None:
    process_step = _default_process_hook("runtime/default-process.yaml")
    workload_step = _default_process_hook("runtime/workload.yaml")
    pack = {
        "steps": [workload_step],
        "process_plans": {
            "__default__": {
                "selector": None,
                "steps": [process_step],
            }
        },
    }
    plan = SimpleNamespace(
        execution=RuntimeExecutionSelection(
            kind="pre_hook",
            selector="orders",
            scope="process",
            process_selector=None,
            hook_name="pre_hook_refresh_orders",
            hook_execution="externalized",
        )
    )

    argv, environment = _selected_command(pack, plan)

    assert argv[3] == "runtime/default-process.yaml"
    assert environment == {}


def _default_process_hook(manifest_path: str) -> dict[str, object]:
    argv = [
        "dpone",
        "hooks",
        "execute",
        manifest_path,
        "--phase",
        "pre_hook",
        "--hook-id",
        "refresh_orders",
    ]
    return {
        "name": "pre_hook_refresh_orders",
        "phase": "pre_hook",
        "command": " ".join(argv),
        "runtime_command": {
            "schema": "dpone.airflow-pre-hook-command.v1",
            "hook_id": "refresh_orders",
            "process_selector": None,
            "argv": argv,
        },
    }
