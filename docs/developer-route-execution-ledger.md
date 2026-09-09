# Developer route execution ledger

The route execution ledger is a control-plane feature. It proves idempotency,
resume safety, commit ordering, and runner fencing for any
`source -> sink -> strategy` route without executing source reads or sink writes.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.execution_models` | Value objects, enums, lease model, step model, decision model, and JSON/Markdown report contract. |
| `dpone.ops.routes.execution_policy` | `RouteCommitProtocolPolicy`, pure commit-order and blocker policy. |
| `dpone.ops.routes.execution_store` | `RouteExecutionLedgerStore` protocol and `LocalRouteExecutionLedgerStore`, deterministic local JSON persistence for steps, index, leases, and best-effort compare-and-swap. |
| `dpone.ops.routes.execution_store_sqlite` | `SqliteRouteExecutionLedgerStore`, SQLite run/lease tables and atomic compare-and-swap append semantics for shared local runners. |
| `dpone.ops.routes.execution_store_factory` | Backend-name composition for `local_json` and `sqlite`; new backends are added here, not in CLI handlers. |
| `dpone.ops.route_execution` | `RouteExecutionService`, dependency-injected composition over store, policy, checksuming, and report writing. |
| `dpone.commands.ops_parsers_core` | CLI argument parser only. No business logic. |
| `dpone.services.ops.command_handlers_release` | Thin handler that invokes the service through the ops catalog and emits JSON/Markdown. |

## Dependency injection

`RouteExecutionService` accepts a `RouteExecutionLedgerStore`, policy, and
clock. Tests inject the clock to make lease expiry deterministic. Future
durable stores should implement the protocol methods from
`dpone.ops.routes.execution_store` and be wired only in a composition root.

Keep dependency injection explicit at service construction time so tests can
swap stores, policies, and clocks without patching global state.

## Commit protocol

The ordered stages are:

1. `planned`
2. `extracting`
3. `loaded_to_staging`
4. `finalized`
5. `quality_checked`
6. `state_committed`

The policy blocks backward stage movement and blocks `state_committed` until a
successful durable sink stage exists in the same ledger. Current durable sink
stages are `loaded_to_staging`, `finalized`, and `quality_checked`.

## Idempotency

Each step has one idempotency key. The service compares a replay candidate with
the existing step fingerprint after removing volatile fields such as timestamp
and runner id:

- exact same fingerprint: warn with `route_execution.idempotent_replay`, do not
  append a duplicate;
- same idempotency key with different payload: block with
  `route_execution.idempotency_conflict`;
- new idempotency key: evaluate through `RouteCommitProtocolPolicy`.

## Compare-and-swap append

The service reads the current step tuple, evaluates policy, and then calls
`RouteExecutionLedgerStore.append_steps_if_version` with the expected step
count. The local JSON store performs a best-effort version check. The SQLite
store performs the same check inside `BEGIN IMMEDIATE`, so only one runner can
append based on a given version.

If the compare-and-swap fails, the service re-reads the ledger:

- if the new state already contains the same idempotency fingerprint, the
  command returns the normal `route_execution.idempotent_replay` warning;
- otherwise it blocks with `route_execution.concurrent_write_conflict`.

## Lease fencing

`LocalRouteExecutionLedgerStore.acquire_lease` and
`SqliteRouteExecutionLedgerStore.acquire_lease` store one lease per route and
dataset. A different owner is blocked while the lease is active. Expired leases
can be taken over by a new owner and the report records the new fencing token.

This local file implementation is intended for CI, local certification, and
single-runner evidence. The SQLite implementation is intended for shared local
runners, Docker-live certification, and release rehearsals that need durable
multi-runner coordination without introducing vendor services.

## SQLite schema

`SqliteRouteExecutionLedgerStore` owns two tables:

| Table | Responsibility |
| --- | --- |
| `route_execution_runs` | One row per `route_case_id + dataset + run_id`, with route JSON, step count, serialized steps, ledger path, and update timestamp. |
| `route_execution_leases` | One row per route/dataset lease with owner, expiration, and fencing token. |

The schema is internal to the store. Do not query it from command handlers or
policy code; use the `RouteExecutionLedgerStore` protocol instead.

## No connector imports

No connector imports are allowed in this feature. The ledger records boundaries,
artifact hashes, and decisions that runtime code or external automation already
produced. Source readers, sink appliers, CDC adapters, and database clients must
stay in runtime or connector packages.

## Extension rules

- Add a new route by updating matrix/profile metadata and docs, not by changing
  command logic.
- Add a new durable stage only in `RouteExecutionStage` and
  `RouteCommitProtocolPolicy`; update docs and tests in the same PR.
- Add a new persistence backend behind a small store interface. Do not add
  cloud, database, or connector SDK imports to the service.
- Add backend-name selection only in `RouteExecutionLedgerStoreFactory` and ops
  catalog composition. Keep `RouteExecutionService` injected and backend-agnostic.
- Keep report fields stable. Add optional fields instead of renaming existing
  JSON keys.
- Keep CLI handlers thin: parse arguments, call the service, emit report.

## Tests

Focused tests live in:

- `tests/test_route_execution_ledger.py`
- `tests/test_route_execution_ledger_sqlite.py`
- `tests/test_cli_route_execution_ledger_command.py`
- `tests/test_route_execution_ledger_docs_contract.py`

Run:

```bash
uv run pytest tests/test_route_execution_ledger.py tests/test_route_execution_ledger_sqlite.py tests/test_cli_route_execution_ledger_command.py tests/test_route_execution_ledger_docs_contract.py -q
uv run ruff check src/dpone/ops/routes/execution_models.py src/dpone/ops/routes/execution_policy.py src/dpone/ops/routes/execution_store.py src/dpone/ops/routes/execution_store_sqlite.py src/dpone/ops/routes/execution_store_factory.py src/dpone/ops/route_execution.py
```
