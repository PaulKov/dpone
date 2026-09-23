"""Pure stage records confer neither Prepared nor write authority."""

import pytest

from dpone.contracts.mssql_sqlclient_stage_observation import snapshot_stage_identity
from tests.test_mssql_sqlclient_stage_identity import stage


def test_stage_snapshot_is_independent_and_equal():
    original = stage()
    copied = snapshot_stage_identity(original)
    assert copied == original and copied is not original
    assert copied.columns[0] is not original.columns[0]


@pytest.mark.parametrize("value", [True, 1.0, None])
def test_snapshot_rejects_original_integer_alias(value):
    original = stage()
    object.__setattr__(original, "object_id", value)
    with pytest.raises(ValueError):
        snapshot_stage_identity(original)


def test_alias_with_coercing_deepcopy_rejected_before_normalization():
    class Text(str):
        def __deepcopy__(self, memo):
            return str(self)

    original = stage()
    object.__setattr__(original.columns[0], "name", Text("value"))
    with pytest.raises(ValueError):
        snapshot_stage_identity(original)
