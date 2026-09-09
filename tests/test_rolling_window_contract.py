"""Public rolling-window parsing and boundary invariants."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from dpone.contracts.rolling_window import RollingWindowSpec


def config(**updates):
    return {"column": "observed_at", "anchor": "data_interval_end", "lookback": "P7D", **updates}


def test_window_freezes_context_once_and_covers_exact_half_open_interval():
    spec = RollingWindowSpec.from_mapping(config(chunk_interval="P2D"))
    context = {"data_interval_end": datetime(2026, 9, 9, tzinfo=UTC)}
    frozen = spec.freeze(context)
    context["data_interval_end"] += timedelta(days=1)
    assert frozen.start == datetime(2026, 9, 2, tzinfo=UTC)
    assert frozen.end == datetime(2026, 9, 9, tzinfo=UTC)
    assert frozen.boundaries == tuple(datetime(2026, 9, day, tzinfo=UTC) for day in (2, 4, 6, 8, 9))
    assert frozen.fingerprint == spec.freeze({"data_interval_end": frozen.end}).fingerprint


def test_timezone_offsets_identify_the_same_utc_window():
    spec = RollingWindowSpec.from_mapping(config())
    utc = spec.freeze({"data_interval_end": "2026-09-09T00:00:00Z"})
    offset = spec.freeze({"data_interval_end": datetime(2026, 9, 9, 3, tzinfo=timezone(timedelta(hours=3)))})
    assert utc == offset


@pytest.mark.parametrize("duration", ["P0D", "P-1D", "P1M", "P1Y", "P", "PT", "", 7, True, "P1DT"])
def test_invalid_or_calendar_duration_is_rejected(duration):
    with pytest.raises(ValueError, match="window"):
        RollingWindowSpec.from_mapping(config(lookback=duration))


@pytest.mark.parametrize(
    "extra",
    [
        {"column": ""},
        {"column": "x; DROP TABLE y"},
        {"column": "a.b"},
        {"anchor": "now"},
        {"timezone": "Europe/Berlin"},
        {"unexpected": 1},
        {"chunk_interval": "P0D"},
        {"chunk_interval": "PT0.0000001S"},
    ],
)
def test_closed_contract_rejects_unknown_or_unsafe_options(extra):
    with pytest.raises(ValueError, match="window"):
        RollingWindowSpec.from_mapping(config(**extra))


@pytest.mark.parametrize(
    "context",
    [{}, {"data_interval_end": None}, {"data_interval_end": "2026-09-09"}, {"data_interval_end": datetime(2026, 9, 9)}],
)
def test_anchor_is_required_and_must_be_timezone_aware(context):
    with pytest.raises(ValueError, match="window"):
        RollingWindowSpec.from_mapping(config()).freeze(context)


def test_excessive_chunk_count_is_rejected_before_allocation():
    with pytest.raises(ValueError, match="chunk"):
        RollingWindowSpec.from_mapping(config(chunk_interval="PT0.000001S")).freeze(
            {"data_interval_end": "2026-09-09T00:00:00Z"}
        )


def test_duration_and_anchor_preserve_microseconds_without_rounding():
    frozen = RollingWindowSpec.from_mapping(config(lookback="PT0.000003S", chunk_interval="PT0.000002S")).freeze(
        {"data_interval_end": "2026-09-09T00:00:00.000003Z"}
    )
    assert frozen.boundaries == tuple(datetime(2026, 9, 9, microsecond=us, tzinfo=UTC) for us in (0, 2, 3))


def test_frozen_window_defensively_copies_boundaries():
    from datetime import datetime, timedelta

    from dpone.contracts.rolling_window import FrozenRollingWindow

    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    boundaries = [start, end]
    frozen = FrozenRollingWindow("at", start, end, boundaries)
    fingerprint = frozen.fingerprint
    boundaries.clear()
    assert frozen.boundaries == (start, end) and frozen.fingerprint == fingerprint


@pytest.mark.parametrize("case", ["empty", "reversed", "gap", "naive", "identifier"])
def test_direct_frozen_window_rejects_invalid_contract(case):
    from datetime import datetime, timedelta

    from dpone.contracts.rolling_window import FrozenRollingWindow

    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    args = ["at", start, end, (start, end)]
    if case == "empty":
        args[3] = ()
    elif case == "reversed":
        args[3] = (end, start)
    elif case == "gap":
        args[3] = (start + timedelta(hours=1), end)
    elif case == "naive":
        args[1] = start.replace(tzinfo=None)
    else:
        args[0] = "at; DROP TABLE x"
    with pytest.raises(ValueError, match="rolling_window"):
        FrozenRollingWindow(*args)
