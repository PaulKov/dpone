# Developer CDC apply certification

CDC apply certification is a credential-free evidence producer for CDC handoff.
It proves that a bounded CDC event window can be applied idempotently, that
delete semantics are explicit, and that typed source/sink hashes match.

The runner does not open live database connections. Live readers remain under
`dpone.runtime.cdc`; replay planning remains under `dpone.readiness.cdc_replay`;
handoff evaluation remains under `dpone.ops.cdc.handoff`.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.cdc.apply_models` | `CdcApplyEvent`, `CdcApplyFixture`, `CdcApplyResult`, `CdcApplyCertificationReport`, and deterministic row hashing helpers. |
| `dpone.ops.cdc.apply` | `CdcApplyStrategy`, `InMemoryCdcApplyStrategy`, and `CdcApplyCertificationService`. |
| `dpone.ops.cdc.handoff` | Consumes generated evidence and writes `cdc_handoff.json`. |
| `dpone.ops.cdc.catalog` | Selects source backend and sink apply profile metadata from route metadata. |

## Interfaces

`CdcApplyEvent` is a normalized event with `operation`, `position`,
`sequence`, `key`, optional `before`, optional `after`, and deterministic
`event_id`.

`CdcApplyFixture` is the input contract. It contains `unique_key`,
`snapshot_boundary`, `window_start`, `window_end`, `retention_min`,
`initial_rows`, `events`, `expected_rows`, and optional `schema_changes`.

`CdcApplyStrategy` is the extension point. It receives a fixture and returns a
`CdcApplyResult`. The default `InMemoryCdcApplyStrategy` is generic: it applies
insert/update/delete events by unique key, skips duplicate event ids, and
compares canonical final rows.

`CdcApplyCertificationService` composes the CDC handoff catalog, apply
strategy, evidence writer, and `SnapshotCdcHandoffService`.

## Extension rules

Do not add route-specific branches to the service. Route-specific behavior
belongs in `CdcRouteMetadata` or in a small injected `CdcApplyStrategy` selected
by `sink_apply_mode`.

When adding a route:

1. Confirm the `source -> sink -> cdc` route exists in the integration matrix.
2. Add or reuse `CdcRouteMetadata`.
3. Add a small `CdcApplyStrategy` only when generic in-memory semantics are not
   enough for the sink apply mode.
4. Add tests proving the new strategy writes `cdc_apply_correctness`,
   `delete_semantics`, and `typed_cdc_hash` evidence.
5. Add user docs, developer docs, source-sink guide examples, and runbooks.
6. Run full CI, docs, import, layer, module-size, and architecture-fitness
   checks.

Keep this package as a control-plane evidence layer. It may parse local JSON
fixtures and write artifacts, but it must not import live connector adapters or
execute source/sink network IO.
