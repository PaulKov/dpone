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
