"""Closed authority versions; v2 is explicit and never migrates a v1 original."""

from enum import StrEnum


class AuthorityVersion(StrEnum):
    V1 = "dpone.clickhouse.authority.v1"
    V2 = "dpone.clickhouse.authority.v2"


_V1 = """
CREATE TABLE authority_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL, deployment TEXT NOT NULL);
CREATE TABLE subjects (subject_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
    owner TEXT NOT NULL UNIQUE, epoch INTEGER NOT NULL CHECK(epoch>0));
CREATE TABLE operations (operation_id TEXT PRIMARY KEY, subject_key TEXT NOT NULL UNIQUE
    REFERENCES subjects(subject_key), candidate TEXT NOT NULL, query_id TEXT NOT NULL UNIQUE,
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0), record TEXT, intent TEXT,
    transport TEXT NOT NULL DEFAULT 'not_started' CHECK(transport IN
      ('not_started','may_have_sent','closed_without_send','closed_terminal')),
    grant_hash TEXT, completion_digest TEXT, observed TEXT);
CREATE TABLE history (operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    revision INTEGER NOT NULL, event TEXT NOT NULL, transport TEXT NOT NULL,
    record_digest TEXT, observed TEXT, observation_digest TEXT, PRIMARY KEY(operation_id,revision));
CREATE TRIGGER history_no_update BEFORE UPDATE ON history BEGIN
    SELECT RAISE(ABORT,'Immutable authority history'); END;
CREATE TRIGGER history_no_delete BEFORE DELETE ON history BEGIN
    SELECT RAISE(ABORT,'Immutable authority history'); END;
CREATE TRIGGER subject_no_update BEFORE UPDATE ON subjects BEGIN
    SELECT RAISE(ABORT,'Retained authority owner'); END;
CREATE TRIGGER subject_no_delete BEFORE DELETE ON subjects BEGIN
    SELECT RAISE(ABORT,'Retained authority owner'); END;
"""

_V2 = (
    _V1
    + """
CREATE TABLE name_reservations (
    deployment TEXT NOT NULL, server TEXT NOT NULL, database_name TEXT NOT NULL,
    physical_name TEXT NOT NULL, operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    role TEXT NOT NULL CHECK(role IN ('target','candidate')),
    PRIMARY KEY(deployment,server,database_name,physical_name));
CREATE TABLE candidate_operations (
    operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id),
    request TEXT NOT NULL, profile_digest TEXT NOT NULL, inventory TEXT NOT NULL,
    invocation_hash TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
    lifecycle TEXT NOT NULL DEFAULT 'registered'
      CHECK(lifecycle IN ('registered','loading','admission_closed','sealed','retained')),
    admission_closed INTEGER NOT NULL DEFAULT 0 CHECK(admission_closed IN (0,1)),
    source_exhausted INTEGER NOT NULL DEFAULT 0 CHECK(source_exhausted IN (0,1)),
    expected TEXT NOT NULL, create_uuid TEXT, retained_reason TEXT);
CREATE TABLE candidate_requests (
    operation_id TEXT NOT NULL REFERENCES candidate_operations(operation_id),
    sequence INTEGER NOT NULL CHECK(sequence>=0), query_id TEXT NOT NULL UNIQUE,
    request TEXT NOT NULL, grant_hash TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
    state TEXT NOT NULL DEFAULT 'not_started' CHECK(state IN
      ('not_started','may_have_sent','closed_without_send','closed_terminal')),
    completion_digest TEXT,
    PRIMARY KEY(operation_id,sequence),
    CHECK ((state='not_started' AND revision=0 AND completion_digest IS NULL)
      OR (state IN ('may_have_sent','closed_without_send') AND revision=1 AND completion_digest IS NULL)
      OR (state='closed_terminal' AND revision=2 AND completion_digest IS NOT NULL
        AND length(completion_digest)=64 AND completion_digest NOT GLOB '*[^0-9a-f]*')));
CREATE TABLE candidate_history (
    operation_id TEXT NOT NULL REFERENCES candidate_operations(operation_id),
    revision INTEGER NOT NULL, event TEXT NOT NULL, query_id TEXT,
    payload TEXT NOT NULL, PRIMARY KEY(operation_id,revision));
CREATE TABLE candidate_seals (
    operation_id TEXT PRIMARY KEY REFERENCES candidate_operations(operation_id),
    payload TEXT NOT NULL);
CREATE TRIGGER reservation_no_update BEFORE UPDATE ON name_reservations BEGIN
    SELECT RAISE(ABORT,'Retained name reservation'); END;
CREATE TRIGGER reservation_no_delete BEFORE DELETE ON name_reservations BEGIN
    SELECT RAISE(ABORT,'Retained name reservation'); END;
CREATE TRIGGER candidate_identity_no_update BEFORE UPDATE OF
    operation_id,request,profile_digest,inventory,invocation_hash ON candidate_operations BEGIN
    SELECT RAISE(ABORT,'Immutable candidate identity'); END;
CREATE TRIGGER candidate_no_delete BEFORE DELETE ON candidate_operations BEGIN
    SELECT RAISE(ABORT,'Retained candidate operation'); END;
CREATE TRIGGER candidate_monotonic BEFORE UPDATE ON candidate_operations
    WHEN NEW.revision != OLD.revision+1
      OR NEW.admission_closed < OLD.admission_closed OR NEW.source_exhausted < OLD.source_exhausted
      OR (OLD.lifecycle='retained' AND NEW.lifecycle!='retained')
      OR (OLD.lifecycle='sealed' AND NEW.lifecycle NOT IN ('sealed','retained'))
      OR (OLD.create_uuid IS NOT NULL AND NEW.create_uuid IS NOT OLD.create_uuid)
    BEGIN SELECT RAISE(ABORT,'Irreversible candidate lifecycle'); END;
CREATE TRIGGER candidate_request_identity_no_update BEFORE UPDATE OF
    operation_id,sequence,query_id,request,grant_hash ON candidate_requests BEGIN
    SELECT RAISE(ABORT,'Immutable candidate request'); END;
CREATE TRIGGER candidate_request_no_delete BEFORE DELETE ON candidate_requests BEGIN
    SELECT RAISE(ABORT,'Retained candidate request'); END;
CREATE TRIGGER candidate_request_monotonic BEFORE UPDATE ON candidate_requests
    WHEN (OLD.state IN ('closed_without_send','closed_terminal') AND
      (NEW.state!=OLD.state OR NEW.completion_digest IS NOT OLD.completion_digest))
      OR (OLD.state='may_have_sent' AND NEW.state NOT IN ('may_have_sent','closed_terminal'))
    BEGIN SELECT RAISE(ABORT,'Irreversible candidate request'); END;
CREATE TRIGGER candidate_history_no_update BEFORE UPDATE ON candidate_history BEGIN
    SELECT RAISE(ABORT,'Immutable candidate history'); END;
CREATE TRIGGER candidate_history_no_delete BEFORE DELETE ON candidate_history BEGIN
    SELECT RAISE(ABORT,'Immutable candidate history'); END;
CREATE TRIGGER candidate_seal_no_update BEFORE UPDATE ON candidate_seals BEGIN
    SELECT RAISE(ABORT,'Immutable candidate seal'); END;
CREATE TRIGGER candidate_seal_no_delete BEFORE DELETE ON candidate_seals BEGIN
    SELECT RAISE(ABORT,'Immutable candidate seal'); END;
"""
)


def schema_sql(version: AuthorityVersion) -> str:
    """Return an immutable closed schema choice; never accept an arbitrary string."""
    if type(version) is not AuthorityVersion:
        raise ValueError("Unsupported authority version")
    return _V1 if version is AuthorityVersion.V1 else _V2
