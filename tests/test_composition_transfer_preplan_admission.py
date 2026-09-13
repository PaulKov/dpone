"""Trusted preplan must precede registration and abort the prepared source on refusal."""

from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.run_context import RunContext
from dpone.runtime.etl.mssql_transaction_admission import MssqlTransactionAdmissionService
from tests.test_mssql_generic_transaction_governance import (
    _admission_sink,
    _AdmissionState,
    _config,
    _source_physical_identity,
)


@pytest.mark.parametrize("reject", [False, True])
def test_preplan_hook_precedes_registration_and_preserves_boundary_cleanup(reject):
    events = []
    boundary = SimpleNamespace(abort_preserving=lambda error: events.append("abort"))
    source = SimpleNamespace(
        connector=object(),
        mssql_transaction_checkpoint_mode=lambda config: MssqlTransactionCheckpointMode.STATELESS,
        mssql_transaction_source_physical_identity=lambda config: _source_physical_identity(),
        prepare_mssql_source_boundary=lambda config: boundary,
        fetch_schema_projection=lambda config: SimpleNamespace(
            projected_schema=(("id", "bigint"),), target_projection=None
        ),
    )
    reference = object()

    def verify(admission, **kwargs):
        assert kwargs["source"] is source and kwargs["prepared_boundary"] is boundary
        assert len(kwargs["submitted_mutation_sha256"]) == 32
        events.append("verified")
        if reject:
            raise ValueError("unapproved mutation")
        return reference

    def register(admission, digest, *, preplan_reference):
        assert preplan_reference is reference and len(digest) == 32
        events.append("registered")

    service = MssqlTransactionAdmissionService(
        target_resolver=lambda *args, **kwargs: SimpleNamespace(
            digest=b"t" * 32, database_name="DWH", schema_name="dbo", table_name="target"
        ),
        state_factory=lambda storage: _AdmissionState([]),
        operation_registrar=register,
        preplan_verifier=verify,
    )

    def prepare():
        return service.prepare(
            _config(),
            source=source,
            sink=_admission_sink(SimpleNamespace(atomicity="target_atomic", provisioning="external")),
            run_context=RunContext("run", config={"process": "p", "pipeline_id": "pipe", "task_id": "task"}),
            load_record=SimpleNamespace(load_id="load"),
            dag_id="dag",
        )

    if reject:
        with pytest.raises(ValueError, match="unapproved"):
            prepare()
        assert events == ["verified", "abort"]
    else:
        prepare()
        assert events == ["verified", "registered"]
