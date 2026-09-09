# Feature design: Public snapshot CI recovery

- Status: APPROVED
- Owner: PaulKov
- Target release: no publication requested
- Last verified: 2026-09-09

## Approval and problem

The maintainer explicitly instructed the agent to fix all identified merge
blockers and complete PR #2 after reviewing the dbt implementation and its
Docker evidence. This authorizes the bounded recovery described here, including
its governance migration; it does not authorize bypassing required checks.
The public repository begins at a new root, while imported policy and debt
metadata still name unavailable historical objects and a deleted ruleset.

## Personas and journey

Maintainers need ordinary protected merges. Contributors need reproducible
checks from the public checkout. They inspect the migration report, run the
producer against the fixed public root, commit its candidate, and run exact-head
checks. Historical proof remains explicitly UNVERIFIED; current snapshot
continuity and current code checks must pass independently.

## Scope and contracts

1. Update only active ruleset identity/version/time to observed live values;
   preserve every required check and protection. Regenerate derived authority
   artifacts through their producer. No provider rule modification.
2. Adopt module debt from public root
   `f8c6a4a5e75d167829c05f65d5d3033acb193878`. The producer reads its exact
   Git ledger and sources, proves all caps equal observed source sizes, and
   changes only `baseline_commit`. It retains original identities in a report.
   No cap, deadline, ownership, ADR or reduction target may loosen.
3. Keep ordinary ancestry checks unchanged. Permit only this fixed, verified
   transition in metadata/null-ADR continuity. Subsequent tightening, retirement
   and rename checks use the ordinary rules. New debt cannot use adoption.
4. Separate unavailable retrospective CI-shadow proof from active regression
   checks. A dedicated historical evaluator remains nonzero/UNVERIFIED when
   objects are missing. Active checks verify immutable imported contracts against
   the public root/tree plus all existing current/synthetic assertions.
5. Correct governance snapshot ABA detection with a lease-owned event observer
   alongside existing byte/stat/path checks. Linux inotify and macOS kqueue
   observe namespace/attribute/content changes before initial reads and through
   final validation. Observation failure/overflow/watch loss fails closed.
   Incomplete acquisition retains only policy-scope observations.

## Algorithm, identity and failure semantics

Resolve the exact public root and require it to be an ancestor of the evaluated
base. Require its frozen ledger digest and per-source measurements; reject
foreign roots, entries, altered metadata, increased caps and missing prior debt.
Produce a candidate and a report with distinct public-observation and historical
proof statuses. Use existing exact-HEAD/byte compare-and-swap baseline writing.
A crash or report-write failure cannot certify the candidate; retry regenerates
from immutable inputs, and the normal exact-head gate is the acceptance boundary.
Rollback restores the previous candidate, which still truthfully fails old
historical ancestry checks. It never restores a false success.

An observer is opened at snapshot acquisition. Register confined file/directory
FDs before reading metadata, inventory or bytes. Any relevant event remains dirty
until finalization; final event polling follows existing revalidation. Close all
resources on success, partial failure and BaseException. Unsupported observers
cannot yield a complete verified snapshot. Observation ends at final poll; remote
filesystem and post-return activity are not covered by a perpetual guarantee.

## Architecture and compatibility

A small metrics adoption policy owns transition eligibility, reusing the v2
codec, Git identity and transactional writer. A tools producer owns command I/O.
The v2 ledger schema is unchanged. A separate historical evaluator cannot be
called by live merge/release authority as a substitute for historical proof.
A narrow event-observer module owns OS resources; the existing snapshot reader
owns policy/workflow scope and evidence classification. No release publisher,
connector, dbt runtime contract or branch protection is weakened.

## Validation and rollout

Red/green tests cover exact adoption, absent/foreign root, ancestry, cap growth,
metadata changes, missing/new debt, idempotence and transactional races; normal
missing-history checks must still fail. Historical evaluator tests distinguish
PASS/UNVERIFIED/failure and detect changed imported artifacts. Existing synthetic
merge/squash tests remain. ABA tests run unchanged repeatedly in Docker, including
hardlinks, overflow, watch loss, reads, finalization mutation and cleanup.
Run scoped governance tests, all required Python/docs/architecture gates, Docker
regressions, then exact-head GitHub checks. Fresh-context review precedes merge.

## Comparison and measurable outcome

Connector/orchestration products dlt, Informatica, Airbyte, Fivetran, Pentaho,
SSIS, gusty, Cosmos and Beam are N/A: this is repository governance recovery,
not a competing integration capability. No superiority claim is made.
Success means ordinary protected merge with truthful public-root provenance,
unchanged debt caps and no ABA false PASS in the reproduced local scenarios.
Evidence will be recorded under `test_artifacts/dbt-compact-wire-v2/`.
