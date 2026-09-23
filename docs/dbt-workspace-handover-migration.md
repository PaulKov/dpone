# Workspace handover gateway migration

This page describes the deployment boundary for the
[approved durable handover design](feature-design-durable-workspace-handover.md).
The implementation is incomplete. The renderers and typed readback are not an
installation command, and they do not enable runtime handover. Do not install an
isolated permission cutover or register a channel before the complete gateway,
compatible callers, migration validator and environment certification are ready.

## Who owns the migration

A platform operator owns the protected control database and reviews the exact
rendered DDL, procedure definitions, role assignments and channel registration
proof. A workload author does not need permission to edit guard rows or create a
registration. Runtime connections must never execute installation SQL.

The same desired object is one shared channel across watcher replicas. Its
identity includes the canonical object URI, registry scope, environment and
source project/ref. A watcher-specific authority hash, hostname or cache path
must not split that channel. Retain the existing desired occurrence UUID.

## Preconditions

- A disposable, explicitly approved SQL certification environment is available.
  No live check is a pass merely because the offline renderer tests pass.
- SQL Server supports `CREATE OR ALTER PROCEDURE`, `OPENJSON` and large
  `HASHBYTES` inputs: the renderer requires SQL Server 2016 SP1 or later and
  database compatibility level 130 or later.
- The schema and fixed gateway modules share a reviewed non-runtime owner.
  Module creation uses `ANSI_NULLS ON` and `QUOTED_IDENTIFIER ON`; the permission
  renderer rejects modules whose recorded settings differ.
- Installation and mutation sessions use the required filtered-index settings:
  `ANSI_NULLS`, `QUOTED_IDENTIFIER`, `ANSI_PADDING`, `ANSI_WARNINGS`, `ARITHABORT`
  and `CONCAT_NULL_YIELDS_NULL` are ON; `NUMERIC_ROUNDABORT` is OFF.
- All existing workspace callers and the bounded semantic guard writer paths
  have been upgraded and old writers drained. Old direct-DML binaries cannot
  run under the migrated roles.
- The retained immutable artifacts and original activation request are available
  for each occurrence that will be adopted. An activation/request hash alone is
  not a recoverable original request.

## Reviewed rollout order

1. Render and inspect the complete additive schema and all fixed procedures in
   an offline review. The witness table renderer intentionally fails if its
   tables already exist; an installer must attest exact catalog shape before
   treating a prior migration as installed. A name-only existence check is not
   an authority check.
2. Install and certify the exact renderer output in the approved disposable
   environment. Procedure batches are separate batches, without embedded `GO`.
   Apply the session settings before creating modules, not only inside them.
3. Verify canonical JSON/UTF-8 parity, closed input rejection, transaction races,
   lost acknowledgements, complete cache loss and effective SQL permissions.
   Preserve the renderer commit and database version with the evidence.
4. Upgrade and drain old writers, then apply the reviewed production permission
   migration only under a separate explicit environment authorization.
5. Produce a complete registration inventory. Review either the explicit empty
   attestation or an exact original ACTIVE adoption baseline. Disjoint unbound
   non-retired occurrences block empty registration too; guard overlap is not a
   valid inventory filter.
6. Register with the separate registrar capability, retain the complete
   database-derived receipt, and enable the fully certified native driver.

Managed runtime, legacy workspace, registrar and semantic guard capabilities
are separate roles. The generated grants do not assign users or certify their
effective permissions. Review inherited roles, alternate credentials and other
callable owner-chained modules. Direct DML, DDL, ownership, impersonation, broad
schema execution or a path through an older mutation module blocks enablement.

## Private JSON parity checks

The following opt-in tests execute only preinstalled private helper procedures;
they do not install DDL or grant access. The certification principal is separate
from ordinary runtime roles, which are denied direct helper execution.

Configure `DPONE_WORKSPACE_JSON_TEST_DSN` through the approved secret delivery
mechanism and set `DPONE_WORKSPACE_JSON_TEST_SCHEMA` to the reviewed test schema.
Do not paste a credential-bearing DSN into logs, shell history or evidence.
After the environment and exact test scope are explicitly approved:

```bash
DPONE_WORKSPACE_JSON_LIVE=1 uv run pytest \
  tests/test_dbt_workspace_mssql_gateway_security.py -m integration_live -q
```

The vectors cover ASCII/BMP/supplementary characters, control characters, slash,
backslash, quotes, nested JSON strings, both existing non-ASCII and new
ASCII-escaped canonical formats, exact raw UTF-8 hashes, duplicate keys, integer
bounds and an oversized-schema digest forgery. Without the enable flag, these
tests report SKIP. Passing them certifies only those primitive cases, not gateway
permissions, registration or end-to-end handover.

Request validation vectors also distinguish the retained original guard ID from
the legacy slash-normalized request fingerprint. Full-width slash/backslash
characters are not ASCII replacements: SQL collation must not change that
meaning. An equal normalized hash does not authorize replacing a retained
request with different original identifiers. The private request validator checks
the complete resource/write partition and the existing physical storage bounds;
it never acquires ownership or changes the request.

## Failure and rollback boundaries

An unregistered channel, changed immutable payload, unknown acknowledgement or
missing terminal receipt is non-success. A local pointer does not prove ACTIVE.
On uncertain acknowledgement, use fresh exact protected readback and the same
occurrence; never generate a replacement UUID or clear a guard manually.

Do not restore broad runtime DML grants as a rollback. Preserve the protected
witness and immutable history, stop the affected reconcile path, and complete
the reviewed recovery or a separately approved authority-mode migration. A
PREPARED occurrence is not an abandoned lease, and RETIRING does not authorize
unconditional release.

See [ADR 0072](adr/0072-durable-workspace-handover-witness.md) for the authority
boundary and [the executor reference](workspace-handover-execution.md) for bounded
application progression. Full runtime recovery remains unavailable until the
remaining gateway and artifact-driver work is certified.
