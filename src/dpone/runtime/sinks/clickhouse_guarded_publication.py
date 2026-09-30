"""One-shot, method-aware publication kernel; no default production binding."""

from dataclasses import replace

from dpone.contracts import clickhouse_publication as publication
from dpone.contracts.clickhouse_publication import PublicationUnknown as PublicationUnknown
from dpone.ports.clickhouse_publication import GuardedPublicationBackend


class GuardedClickHousePublication:
    """Separate orchestration from deployment-owned authority and persistence."""

    def __init__(self, backend: GuardedPublicationBackend) -> None:
        self._backend = backend

    def publish(self, operation_id: str) -> publication.PublicationRecord:
        """New operations may dispatch once; existing operations only recover."""
        with self._backend.hold(operation_id):
            record = self._backend.read(operation_id)
            if record is not None:
                return self._recover(record, operation_id)
            intent = publication.choose_publication(operation_id, self._backend.observe(operation_id))
            record = self._backend.prepare(intent)
            self._validate(record, operation_id)
            if record.intent != intent or record.state != publication.PublicationState.PREPARED:
                raise PublicationUnknown("Protected preparation returned a different intent/state")
            claimed = self._backend.claim(record)
            if claimed is None:
                existing = self._backend.read(operation_id)
                if existing is None:
                    raise PublicationUnknown("Claim lost without a readable original record")
                return self._recover(existing, operation_id)
            self._validate(claimed, operation_id)
            if claimed.intent != intent or claimed.state != publication.PublicationState.CLAIMED:
                raise PublicationUnknown("Protected claim returned a different intent/state")
            try:
                if self._backend.observe(operation_id) != intent.before:
                    raise PublicationUnknown("Original catalog or sealed content changed before dispatch")
                if intent.method != "noop":
                    self._backend.execute_once(intent)
            except Exception:
                # Both pre-dispatch rejection and lost acknowledgement need the
                # same protected closure/reconciliation; neither permits replay.
                pass
            except BaseException:
                # Cancellation must not leave a live publisher if closure can
                # still be proven. Preserve the original cancellation even when
                # recovery fails; durable ownership remains retained either way.
                try:
                    self._recover(claimed, operation_id)
                except BaseException:
                    pass
                raise
            return self._recover(claimed, operation_id)

    def recover(self, operation_id: str) -> publication.PublicationRecord:
        """Source-free reconciliation; never create an intent or execute DDL."""
        with self._backend.hold(operation_id):
            record = self._backend.read(operation_id)
            if record is None:
                raise PublicationUnknown("No durable publication intent exists")
            return self._recover(record, operation_id)

    @staticmethod
    def _validate(record: publication.PublicationRecord, operation_id: str) -> None:
        if (
            record.schema_version != "dpone.clickhouse.guarded-publication.v2"
            or record.intent.operation_id != operation_id
            or not isinstance(record.state, publication.PublicationState)
            or type(record.claim_granted) is not bool
            or record.state in {publication.PublicationState.CLAIMED, publication.PublicationState.COMMITTED}
            and not record.claim_granted
            or record.state == publication.PublicationState.PREPARED
            and record.claim_granted
            or record.intent != publication.choose_publication(operation_id, record.intent.before)
        ):
            raise PublicationUnknown("Invalid or foreign durable publication record")

    def _recover(self, record: publication.PublicationRecord, operation_id: str) -> publication.PublicationRecord:
        self._validate(record, operation_id)
        if record.state in {publication.PublicationState.COMMITTED, publication.PublicationState.NOT_PUBLISHED}:
            return record  # Historical outcome, not a claim about today's table.
        try:
            self._backend.close_and_drain(record.intent)
            observed = self._backend.observe(operation_id)
            observed.require_supported()
            state = _classify(record, observed)
            resolved = self._backend.resolve(record, state, observed)
            self._validate(resolved, operation_id)
            if (
                resolved.intent != record.intent
                or resolved.state != state
                or resolved.claim_granted != record.claim_granted
            ):
                raise PublicationUnknown("Resolution did not preserve the exact intent/state")
        except Exception as error:
            raise PublicationUnknown(
                "Publication closure or resolution is unproven; retain target ownership"
            ) from error
        if state == publication.PublicationState.UNKNOWN:
            raise PublicationUnknown("Catalog/content does not match either frozen generation")
        return resolved


def _classify(
    record: publication.PublicationRecord, observed: publication.PublicationObservation
) -> publication.PublicationState:
    intent = record.intent
    before = intent.before
    if observed.subject != before.subject:
        return publication.PublicationState.UNKNOWN
    old, new = before.target, before.candidate
    if new is None:
        return publication.PublicationState.UNKNOWN
    if observed == before and (not record.claim_granted or intent.method != "noop"):
        return publication.PublicationState.NOT_PUBLISHED
    if not record.claim_granted:
        return publication.PublicationState.UNKNOWN
    if intent.method == "rename":
        desired = replace(before, target=new, candidate=None)
    elif intent.method == "exchange":
        desired = replace(before, target=new, candidate=old)
    elif intent.method == "replace_partition" and old is not None:
        desired = replace(before, target=replace(new, uuid=old.uuid))
    elif intent.method == "noop":
        desired = before
    else:
        return publication.PublicationState.UNKNOWN
    return publication.PublicationState.COMMITTED if observed == desired else publication.PublicationState.UNKNOWN
