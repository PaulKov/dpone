"""Complete observed source bounds; row/byte ceilings before export."""

import pytest

from dpone.contracts.nonproduction_scope import NonproductionAuthorityError


@pytest.mark.parametrize("rows,byte_count", [(0, 0), (1, 17), (100, 1000)])
def test_observed_transport_bound_accepts_exact_limit(rows, byte_count):
    from dpone.contracts.nonproduction_fixture_input import require_fixture_transport_bound

    observed = require_fixture_transport_bound(
        [{"source_rows": rows, "transport_bytes_upper_bound": byte_count}],
        max_rows=rows,
        max_bytes=byte_count,
    )
    assert (observed.source_rows, observed.transport_bytes_upper_bound) == (rows, byte_count)


@pytest.mark.parametrize(
    "row",
    [
        {"source_rows": 2, "transport_bytes_upper_bound": 10},
        {"source_rows": 1, "transport_bytes_upper_bound": 11},
        {"source_rows": True, "transport_bytes_upper_bound": 10},
        {"source_rows": 1, "transport_bytes_upper_bound": "10"},
    ],
)
def test_observed_transport_bound_rejects_n_plus_one_and_coercion(row):
    from dpone.contracts.nonproduction_fixture_input import require_fixture_transport_bound

    with pytest.raises(NonproductionAuthorityError):
        require_fixture_transport_bound([row], max_rows=1, max_bytes=10)


@pytest.mark.parametrize(
    "records",
    [
        [],
        [{}, {}],
        None,
        {},
        [{"source_rows": 0, "transport_bytes_upper_bound": 1}],
        [{"source_rows": 1, "transport_bytes_upper_bound": 0}],
        [{"source_rows": -1, "transport_bytes_upper_bound": 1}],
        [{"source_rows": 2**63, "transport_bytes_upper_bound": 1}],
    ],
)
def test_complete_bound_observation_shape_and_empty_consistency(records):
    from dpone.contracts.nonproduction_fixture_input import require_fixture_transport_bound

    with pytest.raises(NonproductionAuthorityError):
        require_fixture_transport_bound(records, max_rows=100, max_bytes=100)
