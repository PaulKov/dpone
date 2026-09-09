from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

import pytest

from dpone.contracts.airflow_desired_state import (
    AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA,
    AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA,
    MAX_AIRFLOW_DESIRED_STATE_BYTES,
    AirflowDesiredDeployment,
    AirflowDesiredStateError,
    DesiredStatePublishEvidence,
    DesiredStateRevision,
)
from dpone.contracts.airflow_desired_state_publish import (
    DesiredStatePublishCandidate,
    DesiredStatePublishIntent,
    DesiredStatePublishRequest,
)
from dpone.ports.airflow_desired_state import (
    DesiredStateConditionalWriteConflict,
    DesiredStateReadResult,
    DesiredStateReadUnavailable,
    DesiredStateSourceUnauthorized,
    DesiredStateWriteContention,
    DesiredStateWriteResult,
    DesiredStateWriteUncertain,
)
from dpone.services.airflow_desired_state import (
    AirflowDesiredStatePublisher,
    DesiredStatePublishError,
)

_DIGEST_A = "sha256:" + "a" * 64
_DIGEST_B = "sha256:" + "b" * 64
_DIGEST_C = "sha256:" + "c" * 64
_DIGEST_D = "sha256:" + "d" * 64
_DIGEST_E = "sha256:" + "e" * 64
_AUTHORITY_DIGEST = "sha256:" + "f" * 64
_OCCURRENCE = "123e4567-e89b-42d3-a456-426614174000"
_PREVIOUS_OCCURRENCE = "223e4567-e89b-42d3-a456-426614174000"


def _desired_payload(
    *,
    occurrence_id: str = _OCCURRENCE,
    previous_revision: str | None = None,
    previous_deployment_id: str | None = None,
    deployment_id: str = _DIGEST_B,
) -> dict[str, object]:
    return {
        "schema": AIRFLOW_DESIRED_DEPLOYMENT_SCHEMA,
        "environment": "dev",
        "source": {
            "project": "group/repository",
            "ref": "master",
            "pipeline_id": "123",
            "job_id": "456",
            "occurrence_id": occurrence_id,
            "git_sha": "1" * 40,
        },
        "promotion": {
            "registry_scope_id": _DIGEST_E,
            "release_id": _DIGEST_A,
            "deployment_id": deployment_id,
            "airflow_index_sha256": _DIGEST_C,
            "runtime_image_digest": _DIGEST_D,
            "expected_dag_ids": ["z_dag", "a_dag"],
            "publication_evidence_sha256": _DIGEST_E,
        },
        "previous": {
            "revision": previous_revision,
            "deployment_id": previous_deployment_id,
        },
        "promoted_at": "2026-07-28T09:10:11Z",
    }


def _request(expected_revision: DesiredStateRevision | None = None) -> DesiredStatePublishRequest:
    candidate = DesiredStatePublishCandidate(
        environment="dev",
        project="group/repository",
        source_ref="master",
        pipeline_id="123",
        job_id="456",
        git_sha="1" * 40,
        registry_scope_id=_DIGEST_E,
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        airflow_index_sha256=_DIGEST_C,
        runtime_image_digest=_DIGEST_D,
        expected_dag_ids=("z_dag", "a_dag"),
        publication_evidence_sha256=_DIGEST_E,
        expected_revision=expected_revision,
        authority_sha256=_AUTHORITY_DIGEST,
    )
    return DesiredStatePublishRequest(
        candidate=candidate,
        intent=DesiredStatePublishIntent(
            candidate_sha256=candidate.sha256,
            occurrence_id=_OCCURRENCE,
            promoted_at="2026-07-28T09:10:11Z",
        ),
        publisher_job_id="789",
    )


def test_v07324_python_publish_contracts_remain_constructible() -> None:
    candidate = DesiredStatePublishCandidate(
        environment="dev",
        project="group/repository",
        source_ref="master",
        pipeline_id="123",
        job_id="456",
        git_sha="1" * 40,
        registry_scope_id=_DIGEST_E,
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        airflow_index_sha256=_DIGEST_C,
        runtime_image_digest=_DIGEST_D,
        expected_dag_ids=("a_dag",),
        publication_evidence_sha256=_DIGEST_E,
        expected_revision=None,
    )
    intent = DesiredStatePublishIntent(
        candidate_sha256=candidate.sha256,
        occurrence_id=_OCCURRENCE,
        promoted_at="2026-07-28T09:10:11Z",
    )
    request = DesiredStatePublishRequest(candidate=candidate, intent=intent)
    evidence = DesiredStatePublishEvidence(
        outcome="created",
        environment="dev",
        occurrence_id=_OCCURRENCE,
        release_id=_DIGEST_A,
        deployment_id=_DIGEST_B,
        desired_state_sha256=_DIGEST_C,
        previous_revision=None,
        committed_revision=DesiredStateRevision('"created"'),
        published_at="2026-07-28T09:10:11Z",
    )

    assert request.publisher_job_id is None
    assert candidate.authority_sha256 is None
    assert evidence.schema == AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA
    assert evidence.to_dict()["schema"] == "dpone.airflow-desired-state-publish.v1"
    assert "preparation_job_id" not in evidence.to_dict()
    assert "publisher_job_id" not in evidence.to_dict()


def test_v07324_publish_evidence_positional_schema_remains_compatible() -> None:
    evidence = DesiredStatePublishEvidence(
        "created",
        "dev",
        _OCCURRENCE,
        _DIGEST_A,
        _DIGEST_B,
        _DIGEST_C,
        None,
        DesiredStateRevision('"created"'),
        "2026-07-28T09:10:11Z",
        False,
        AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA,
    )

    assert evidence.schema == AIRFLOW_DESIRED_STATE_PUBLISH_SCHEMA
    assert evidence.preparation_job_id is None
    assert evidence.publisher_job_id is None


class _AllowSource:
    def authorize(self, *, project: str, git_sha: str) -> None:
        assert project == "group/repository"
        assert git_sha == "1" * 40


class _RejectSource:
    def authorize(self, *, project: str, git_sha: str) -> None:
        del project, git_sha
        raise DesiredStateSourceUnauthorized("not protected head")


class _Reader:
    def __init__(self, *results: DesiredStateReadResult | Exception) -> None:
        self.results = list(results)
        self.calls: list[tuple[int, DesiredStateRevision | None]] = []

    def read(
        self,
        *,
        max_bytes: int,
        if_changed_from: DesiredStateRevision | None = None,
    ) -> DesiredStateReadResult:
        self.calls.append((max_bytes, if_changed_from))
        item = self.results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _Writer:
    def __init__(
        self,
        *outcomes: DesiredStateWriteResult | Exception,
        on_write: Callable[[bytes], None] | None = None,
    ) -> None:
        self.outcomes = list(outcomes)
        self.on_write = on_write
        self.calls: list[tuple[str, DesiredStateRevision | None, bytes]] = []

    def create_if_absent(self, body: bytes) -> DesiredStateWriteResult:
        return self._write("create", None, body)

    def replace_if_revision(
        self,
        expected_revision: DesiredStateRevision,
        body: bytes,
    ) -> DesiredStateWriteResult:
        return self._write("replace", expected_revision, body)

    def _write(
        self,
        operation: str,
        expected_revision: DesiredStateRevision | None,
        body: bytes,
    ) -> DesiredStateWriteResult:
        self.calls.append((operation, expected_revision, body))
        if self.on_write is not None:
            self.on_write(body)
        item = self.outcomes.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _publisher(
    reader: _Reader,
    writer: _Writer,
) -> AirflowDesiredStatePublisher:
    return AirflowDesiredStatePublisher(
        reader=reader,
        writer=writer,
        source_authorizer=_AllowSource(),
        max_write_attempts=4,
    )


def test_publisher_checks_protected_source_immediately_before_mutation() -> None:
    writer = _Writer(DesiredStateWriteResult(DesiredStateRevision('"unused"')))
    publisher = AirflowDesiredStatePublisher(
        reader=_Reader(DesiredStateReadResult.absent()),
        writer=writer,
        source_authorizer=_RejectSource(),
    )

    with pytest.raises(DesiredStatePublishError) as failed:
        publisher.publish(_request())

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_SOURCE_UNAUTHORIZED"
    assert writer.calls == []


def test_desired_deployment_is_canonical_strict_and_deterministic() -> None:
    model = AirflowDesiredDeployment.from_mapping(_desired_payload())

    assert model.promotion.expected_dag_ids == ("a_dag", "z_dag")
    assert model.to_json_bytes() == (
        b'{"environment":"dev","previous":{"deployment_id":null,"revision":null},'
        b'"promoted_at":"2026-07-28T09:10:11Z","promotion":{"airflow_index_sha256":"'
        + _DIGEST_C.encode()
        + b'","deployment_id":"'
        + _DIGEST_B.encode()
        + b'","expected_dag_ids":["a_dag","z_dag"],"publication_evidence_sha256":"'
        + _DIGEST_E.encode()
        + b'","registry_scope_id":"'
        + _DIGEST_E.encode()
        + b'","release_id":"'
        + _DIGEST_A.encode()
        + b'","runtime_image_digest":"'
        + _DIGEST_D.encode()
        + b'"},"schema":"dpone.airflow-desired-deployment.v1","source":{"git_sha":"'
        + b"1" * 40
        + b'","job_id":"456","occurrence_id":"123e4567-e89b-42d3-a456-426614174000",'
        b'"pipeline_id":"123","project":"group/repository","ref":"master"}}'
    )
    assert AirflowDesiredDeployment.from_json(model.to_json_bytes()) == model


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update({"extra": "value"}), "unsupported fields"),
        (lambda value: value.update({"access_token": "unsafe"}), "secret-like"),
        (lambda value: value["source"].update({"git_sha": "ABC"}), "git_sha"),
        (lambda value: value["promotion"].update({"deployment_id": "sha256:ABC"}), "deployment_id"),
        (lambda value: value["promotion"].update({"expected_dag_ids": []}), "1..5000"),
        (lambda value: value["source"].update({"project": "../repo"}), "project"),
        (lambda value: value.update({"promoted_at": "2026-07-28T12:10:11+03:00"}), "promoted_at"),
        (lambda value: value["previous"].update({"revision": '"etag"'}), "previous"),
    ],
)
def test_desired_deployment_rejects_unsafe_or_malformed_fields(
    mutate: Callable[[dict[str, object]], None],
    message: str,
) -> None:
    payload = _desired_payload()
    mutate(payload)

    with pytest.raises(AirflowDesiredStateError, match=message):
        AirflowDesiredDeployment.from_mapping(payload)


def test_desired_deployment_rejects_duplicate_noncanonical_and_oversized_json() -> None:
    with pytest.raises(AirflowDesiredStateError, match="duplicate JSON key"):
        AirflowDesiredDeployment.from_json(b'{"schema":"one","schema":"two"}')

    canonical = AirflowDesiredDeployment.from_mapping(_desired_payload()).to_json_bytes()
    with pytest.raises(AirflowDesiredStateError, match="canonical JSON"):
        AirflowDesiredDeployment.from_json(b" " + canonical)

    with pytest.raises(AirflowDesiredStateError, match="72 KiB"):
        AirflowDesiredDeployment.from_json(b"x" * (MAX_AIRFLOW_DESIRED_STATE_BYTES + 1))


def test_revision_is_opaque_bounded_and_never_normalized_as_a_checksum() -> None:
    revision = DesiredStateRevision('"multipart-etag-7"')

    assert revision.value == '"multipart-etag-7"'
    assert str(revision) == '"multipart-etag-7"'
    with pytest.raises(AirflowDesiredStateError, match="revision"):
        DesiredStateRevision("")


def test_publisher_creates_absent_state_and_emits_exact_evidence() -> None:
    reader = _Reader(DesiredStateReadResult.absent())
    writer = _Writer(DesiredStateWriteResult(DesiredStateRevision('"created-etag"')))

    evidence = _publisher(reader, writer).publish(_request())

    assert [(operation, revision) for operation, revision, _ in writer.calls] == [("create", None)]
    published = AirflowDesiredDeployment.from_json(writer.calls[0][2])
    assert published.previous.revision is None
    assert evidence.outcome == "created"
    assert evidence.committed_revision.value == '"created-etag"'
    assert evidence.state_may_have_changed is False
    assert evidence.to_dict()["schema"] == "dpone.airflow-desired-state-publish.v2"
    assert evidence.to_dict()["status"] == "published"
    assert evidence.preparation_job_id == "456"
    assert evidence.publisher_job_id == "789"


def test_publisher_replaces_only_the_exact_opaque_revision() -> None:
    revision = DesiredStateRevision('"multipart-etag-7"')
    current = AirflowDesiredDeployment.from_mapping(
        _desired_payload(occurrence_id=_PREVIOUS_OCCURRENCE, deployment_id=_DIGEST_A)
    )
    reader = _Reader(DesiredStateReadResult.present(current.to_json_bytes(), revision))
    writer = _Writer(DesiredStateWriteResult(DesiredStateRevision('"next"')))

    evidence = _publisher(reader, writer).publish(_request(revision))

    assert writer.calls[0][0:2] == ("replace", revision)
    published = AirflowDesiredDeployment.from_json(writer.calls[0][2])
    assert published.previous.revision == revision
    assert published.previous.deployment_id == _DIGEST_A
    assert evidence.outcome == "replaced"


def test_publisher_treats_equal_existing_occurrence_as_idempotent_replay() -> None:
    initial_reader = _Reader(DesiredStateReadResult.absent())
    initial_writer = _Writer(DesiredStateWriteResult(DesiredStateRevision('"created"')))
    _publisher(initial_reader, initial_writer).publish(_request())
    body = initial_writer.calls[0][2]
    replay_reader = _Reader(DesiredStateReadResult.present(body, DesiredStateRevision('"created"')))
    replay_writer = _Writer()

    retried_request = replace(_request(), publisher_job_id="790")
    evidence = _publisher(replay_reader, replay_writer).publish(retried_request)

    assert evidence.outcome == "idempotent"
    assert evidence.preparation_job_id == "456"
    assert evidence.publisher_job_id == "790"
    assert replay_writer.calls == []


def test_publisher_rejects_stale_guard_and_terminal_conditional_conflict() -> None:
    current_revision = DesiredStateRevision('"current"')
    current = AirflowDesiredDeployment.from_mapping(
        _desired_payload(occurrence_id=_PREVIOUS_OCCURRENCE, deployment_id=_DIGEST_A)
    )
    reader = _Reader(DesiredStateReadResult.present(current.to_json_bytes(), current_revision))
    writer = _Writer(DesiredStateWriteResult(DesiredStateRevision('"unused"')))

    with pytest.raises(DesiredStatePublishError, match="revision") as stale:
        _publisher(reader, writer).publish(_request(DesiredStateRevision('"stale"')))
    assert stale.value.code == "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT"
    assert writer.calls == []

    reader = _Reader(DesiredStateReadResult.absent())
    writer = _Writer(DesiredStateConditionalWriteConflict("conditional write lost"))
    with pytest.raises(DesiredStatePublishError) as conflict:
        _publisher(reader, writer).publish(_request())
    assert conflict.value.code == "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT"
    assert conflict.value.state_may_have_changed is False


def test_publisher_rejects_current_state_for_another_environment() -> None:
    revision = DesiredStateRevision('"current"')
    payload = _desired_payload(occurrence_id=_PREVIOUS_OCCURRENCE)
    payload["environment"] = "prod"
    current = AirflowDesiredDeployment.from_mapping(payload)
    reader = _Reader(DesiredStateReadResult.present(current.to_json_bytes(), revision))
    writer = _Writer()

    with pytest.raises(DesiredStatePublishError) as mismatch:
        _publisher(reader, writer).publish(_request(revision))

    assert mismatch.value.code == "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY"
    assert writer.calls == []


def test_publisher_reconciles_contention_only_while_predecessor_is_unchanged() -> None:
    revision = DesiredStateRevision('"current"')
    current = AirflowDesiredDeployment.from_mapping(
        _desired_payload(occurrence_id=_PREVIOUS_OCCURRENCE, deployment_id=_DIGEST_A)
    )
    snapshot = DesiredStateReadResult.present(current.to_json_bytes(), revision)
    reader = _Reader(snapshot, snapshot)
    writer = _Writer(
        DesiredStateWriteContention("retryable contention"),
        DesiredStateWriteResult(DesiredStateRevision('"winner"')),
    )

    evidence = _publisher(reader, writer).publish(_request(revision))

    assert [call[0] for call in writer.calls] == ["replace", "replace"]
    assert evidence.committed_revision.value == '"winner"'


@pytest.mark.parametrize(
    "write_failure",
    [DesiredStateWriteContention("contention"), DesiredStateWriteUncertain("timeout")],
)
def test_publisher_never_retries_against_a_changed_predecessor(
    write_failure: Exception,
) -> None:
    revision = DesiredStateRevision('"current"')
    current = AirflowDesiredDeployment.from_mapping(
        _desired_payload(occurrence_id=_PREVIOUS_OCCURRENCE, deployment_id=_DIGEST_A)
    )
    winner = AirflowDesiredDeployment.from_mapping(
        _desired_payload(
            occurrence_id="323e4567-e89b-42d3-a456-426614174000",
            deployment_id=_DIGEST_C,
        )
    )
    reader = _Reader(
        DesiredStateReadResult.present(current.to_json_bytes(), revision),
        DesiredStateReadResult.present(winner.to_json_bytes(), DesiredStateRevision('"winner"')),
    )
    writer = _Writer(write_failure)

    with pytest.raises(DesiredStatePublishError) as conflict:
        _publisher(reader, writer).publish(_request(revision))

    assert conflict.value.code == "DPONE_AIRFLOW_DESIRED_STATE_CONFLICT"
    assert len(writer.calls) == 1


def test_publisher_reconciles_ambiguous_write_by_exact_canonical_bytes() -> None:
    reader = _Reader(DesiredStateReadResult.absent())

    def expose_committed_body(body: bytes) -> None:
        reader.results.append(DesiredStateReadResult.present(body, DesiredStateRevision('"committed-after-timeout"')))

    writer = _Writer(DesiredStateWriteUncertain("timeout"), on_write=expose_committed_body)

    evidence = _publisher(reader, writer).publish(_request())

    assert evidence.outcome == "idempotent"
    assert evidence.committed_revision.value == '"committed-after-timeout"'
    assert evidence.state_may_have_changed is False


def test_publisher_reconciles_unknown_writer_failure_after_dispatch() -> None:
    reader = _Reader(DesiredStateReadResult.absent())

    def expose_committed_body(body: bytes) -> None:
        reader.results.append(
            DesiredStateReadResult.present(
                body,
                DesiredStateRevision('"committed-after-unknown-failure"'),
            )
        )

    writer = _Writer(
        RuntimeError("dependency payload"),
        on_write=expose_committed_body,
    )

    evidence = _publisher(reader, writer).publish(_request())

    assert evidence.outcome == "idempotent"
    assert evidence.committed_revision.value == '"committed-after-unknown-failure"'


def test_publisher_reports_unreconciled_ambiguous_write_truthfully() -> None:
    reader = _Reader(
        DesiredStateReadResult.absent(),
        DesiredStateReadUnavailable("read unavailable"),
    )
    writer = _Writer(DesiredStateWriteUncertain("timeout"))

    with pytest.raises(DesiredStatePublishError) as uncertain:
        _publisher(reader, writer).publish(_request())

    assert uncertain.value.code == "DPONE_AIRFLOW_DESIRED_STATE_UNCERTAIN"
    assert uncertain.value.state_may_have_changed is True


def test_publisher_never_redispatches_an_ambiguous_write_after_absent_readback() -> None:
    reader = _Reader(
        DesiredStateReadResult.absent(),
        DesiredStateReadResult.absent(),
    )
    writer = _Writer(
        DesiredStateWriteUncertain("timeout"),
        DesiredStateConditionalWriteConflict("late committed first write"),
    )

    with pytest.raises(DesiredStatePublishError) as uncertain:
        _publisher(reader, writer).publish(_request())

    assert uncertain.value.code == "DPONE_AIRFLOW_DESIRED_STATE_UNCERTAIN"
    assert uncertain.value.state_may_have_changed is True
    assert len(writer.calls) == 1
