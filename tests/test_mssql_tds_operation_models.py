"""Identity and scope contracts for the shared operation-model boundary."""

from __future__ import annotations

import pickle

from dpone.contracts import mssql_tds_api
from dpone.contracts import mssql_tds_operation_models as models


def test_operation_models_preserve_existing_contract_identity_and_pickle() -> None:
    shared = {
        name for name in models.__all__ if hasattr(mssql_tds_api, name) and isinstance(getattr(models, name), type)
    }
    assert shared
    for name in shared:
        value = getattr(models, name)
        assert value is getattr(mssql_tds_api, name), name
        assert pickle.loads(pickle.dumps(value, protocol=5)) is value


def test_operation_models_exclude_codecs_digests_and_validators() -> None:
    forbidden_prefixes = ("decode_", "encode_", "validate_")
    assert not any(name.startswith(forbidden_prefixes) for name in models.__all__)
    assert not any(name.endswith("_digest") for name in models.__all__)
