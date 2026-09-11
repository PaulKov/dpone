# ADR 0061: Qualification and execution share one physical ownership journal

- Status: Accepted
- Date: 2026-09-10
- Scope: Implementation of the approved nonproduction composition authority

## Context

Qualification precedes native/parent compilation and activation. The initial,
unmerged composition control schema ties domains, attempts and gate journals to
an activation. Using a fabricated activation for qualification would misrepresent
its authority. A separate qualification lock table would let current execution
paths overlook its retained owners and unknown operations.

## Decision

Use protected composition schema version 2 with one closed owner/operation core.
Owners are exactly real execution activations or real qualification runs. Every
current v3 prepare, active read, attempt reservation, issuance phase, recovery and
retirement checks this complete shared authority. Preserve the physical guard
formula, control database/principal and global application-lock resource.

Execution adapters project unchanged request, attempt and proof documents from
the shared records. Qualification has explicit internal owner/operation records
bound to its original grant, plans, work item and actual runner invocation. These
records add no external authority family and cannot substitute for authentication,
physical enrollment or a real plan. Existing production/native-v2 readers and
control systems remain unchanged.

A sealed qualification retains every required source and target domain while
signing and compilation run. A later explicit transaction transfers ownership to
the real execution occurrence only after every predecessor operation is closed,
quiescent and reconciled. It advances fencing epochs and preserves original source
seals. No intermediate unowned interval, expiry-based release or blind retry is
allowed. PostgreSQL source ownership is part of the complete internal scope;
unchanged execution receipts continue to project their exact write partition.

The first implementation slice replaces the unmerged v3 storage layout and all
its execution paths together. Qualification issuance, source sealing, transfer
and public scoped factories remain closed until their complete implementations
and real backend evidence exist. See the [detailed design](../composition-shared-ownership.md)
and [approved parent specification](../feature-specs/nonproduction-composition-authority.md).

## Consequences

The new core avoids independently writable activation/qualification states and
requires coordinated SQL catalog, gate and recovery changes. Earlier schema-v1
component evidence remains historical; exact schema-v2 runs must replace it.
Runtime supports one explicit version and rejects old/partial layouts before
mutation. There is no fallback, dual write or parallel control database for the
same physical participants.

The initial deployment is restricted to newly isolated physical incarnations.
Existing ledgers are never automatically migrated, cleared or adopted. A future
offline migrator must preserve all originals, owners, epochs, trust and charges;
active or unknown state cannot be discarded. Rollback cannot reopen old admission
over retained v2 ownership. No migration or publication is authorized by this ADR.

Independent design review and exact execution/compatibility checks are required
before integration. Architectural budgets and missing live evidence remain
release blockers under the existing rules.

## Scoped execution originals and full-scope transfer

Accepted addendum, 2026-09-11, within the approved nonproduction feature. The
initial implementation is an internal pure codec prerequisite, currently an
isolated candidate awaiting architecture qualification and integration. Its
modules are absent from the integrated branch at `dfccfad`. Current schema-v2
adapters continue rejecting its new envelope; coordinated storage and lifecycle
support require their own concrete contracts and backend evidence.

**Scoped execution originals and full-scope transfer.** Nonproduction execution uses an explicit immutable scoped owner envelope within the existing shared execution-owner family. The complete envelope's digest E is the owner subject; its embedded unchanged NP activation request keeps digest R. Existing attempt and legacy write-proof originals keep their original formats and meanings. All owner and operation persistence projections explicitly distinguish E from R and derive full retained/selected partitions from the mandatory original envelope; missing attachments cannot downgrade authority.

The initial qualification-to-execution handoff preserves the exact complete authenticated campaign scope and participant directional subjects, physical domains and claim roles, including qualification-only fixture/source/helper/state effects. Those retained writes do not become execution workload permissions. Actual execution effects and the total genuine legacy-write bridge are separately reconstructed from pinned executable originals. Qualification and execution observations are distinct actual originals; neither is fabricated from signed declarations. An explicit scoped object grammar represents real native table/view and PostgreSQL table/enum/sequence identities; the fixed two-fixture qualification grammar does not certify unsupported native recipes.

Every initial transferred domain advances exactly one epoch in the same atomic direct-owner transition, with no additional acquisition or unowned interval. Broader acquisitions mentioned in general architectural prose are an unimplemented future extension requiring their own authenticated qualification semantics, not a fallback in this handoff. Evidence references form an acyclic graph. Retry requires exact committed readback, complete proof of absence, or blocking unknown state. A mandatory full scoped terminal association will close all selected read/write effects and continuing barriers in addition to the unchanged write proof; the pure envelope alone enables none of these runtime capabilities.


The exact closed transfer cells also require their known source read: generated
MSSQL-to-ClickHouse workloads retain at least one MSSQL table/view read, and
ordinary PostgreSQL-to-MSSQL workloads retain at least one PostgreSQL table read.
The declared effect's purpose does not alter its access or physical guard.
Native constant dbt models have no artificial source-read minimum. Exact actual
source identity and complete executable effects still require protected
reconstruction; the structural minimum grants no access or generation proof.

A native predecessor may create a downstream transfer's current generation.
The runtime must establish that generation's finite bound and writer closure
against the actual executable dependency order. Retained qualification seals and
history do not prove the contents of a later legitimately changed generation.
