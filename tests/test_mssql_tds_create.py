"""Typed CREATE request binds effects without recursive command hashing."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_create import TdsCreateColumn, TdsCreateObservedColumn, TdsCreateRequest, TdsCreateType
from tests.test_mssql_tds_directory import PARENT


def request():
    return TdsCreateRequest(PARENT, UUID(int=9), (TdsCreateColumn("value", TdsCreateType.BIGINT, True),))


def test_request_has_no_coordinator_identity_or_command_hash():
    value = request()
    assert value.parent == PARENT and len(value.columns) == 1
    assert not hasattr(value, "command_sha256")


@pytest.mark.parametrize("name", ["", "x" * 129, "x\0", "\ud800", "😀" * 65, "x\u0085", "x\u009b"])
def test_identifier_bounds_apply_before_effects(name):
    with pytest.raises(ValueError):
        TdsCreateColumn(name, TdsCreateType.BIGINT, True)


def test_unicode_utf16_bound_and_quoted_identifier_is_not_normalized():
    assert TdsCreateColumn("😀" * 64, TdsCreateType.BIGINT, False).name == "😀" * 64
    assert TdsCreateColumn("MiX.]Name", TdsCreateType.BIGINT, True).name == "MiX.]Name"


@pytest.mark.parametrize("bad", [(), tuple(TdsCreateColumn(str(i), TdsCreateType.BIGINT, False) for i in range(101))])
def test_width_is_a_separate_component_profile(bad):
    with pytest.raises(ValueError, match="component_width"):
        replace(request(), columns=bad)
    assert (
        len(
            replace(
                request(), columns=tuple(TdsCreateColumn(str(i), TdsCreateType.BIGINT, False) for i in range(100))
            ).columns
        )
        == 100
    )


@pytest.mark.parametrize(
    "type_,profile,collation",
    [
        (TdsCreateType.BIGINT, (8, 19, 0), None),
        (TdsCreateType.FLOAT53, (8, 53, 0), None),
        (TdsCreateType.NVARCHARMAX, (-1, 0, 0), "Latin1_General_100_CI_AS_SC"),
        (TdsCreateType.DATETIME2_6, (8, 26, 6), None),
    ],
)
@pytest.mark.parametrize("nullable", [True, False])
def test_exact_observed_profiles(type_, profile, collation, nullable):
    column = TdsCreateObservedColumn(1, "x", type_, nullable, *profile, collation)
    for field in ("max_length", "precision", "scale"):
        with pytest.raises(ValueError):
            replace(column, **{field: getattr(column, field) + 1})


@pytest.mark.parametrize(
    "change",
    [
        {"columns": (TdsCreateColumn("x", TdsCreateType.BIGINT, False),) * 2},
        {"object_nonce": UUID(int=0)},
        {"parent": replace(PARENT, table="#temporary")},
    ],
)
def test_request_rejects_invalid_effect_inputs(change):
    with pytest.raises(ValueError):
        replace(request(), **change)
