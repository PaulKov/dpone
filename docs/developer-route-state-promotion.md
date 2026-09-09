# Developer route state promotion

Route state promotion is a control-plane feature. It proves that source state is
advanced only after route execution evidence and a durable sink commit receipt
agree.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.routes.state_promotion_models` | `RouteCommitReceipt`, `RouteStateRecord`, schema version, and JSON/Markdown report contract. |
| `dpone.ops.routes.state_promotion_policy` | `RouteStatePromotionPolicy`, `RouteStatePromotionDecision`, and fail-closed blocker policy over ledger payloads and receipts. |
| `dpone.ops.routes.state_store` | `RouteStateStore` protocol and `LocalRouteStateStore`, including best-effort compare-and-swap. |
| `dpone.ops.routes.state_store_sqlite` | `SqliteRouteStateStore`, SQLite state table, and atomic compare-and-swap writes. |
| `dpone.ops.routes.state_store_factory` | Backend-name composition for `local_json` and `sqlite`. |
| `dpone.ops.route_state_promotion` | `RouteStatePromotionService`, dependency-injected orchestration over policy, state store, ledger JSON, and report writing. |
| `dpone.commands.ops_parsers_core` | CLI parser only. |
| `dpone.services.ops.command_handlers_release` | Thin handler that invokes the ops catalog and emits JSON/Markdown. |

## Dependency injection

`RouteStatePromotionService` accepts a `RouteStateStore`, policy, and clock.
Tests inject stores and clocks directly. Production composition goes through
`ReadinessOpsCatalog.route_state_promotion`, which uses
`RouteStateStoreFactory`.

Keep dependency injection explicit. Do not read environment variables, create
connectors, or select backends inside the service.

## Policy

`RouteStatePromotionPolicy` blocks state movement when:

- the route execution ledger did not pass;
- route, dataset, or run id do not match;
- no successful durable sink ledger step exists;
- commit receipt source/sink boundaries do not match durable ledger evidence;
- the ledger lease fencing token does not match the receipt fencing token;
- idempotency or compare-and-swap checks fail.

## State stores

`RouteStateStore` is the persistence port. It has three operations:

- `read_state`
- `write_state_if_version`
- `state_path`

`LocalRouteStateStore` writes `route_state_store.json` and is intended for
single-runner evidence. `SqliteRouteStateStore` writes
`route_state_store.sqlite3` and uses `BEGIN IMMEDIATE` to make version checks
atomic for shared local runners.

## No connector imports

No connector imports are allowed in this feature. The service does not advance
state in MSSQL, Postgres, ClickHouse, Kafka, or external APIs. It writes a
control-plane state artifact that runtime adapters can consume through their own
state-store ports.

## Extension rules

- Add a new backend behind `RouteStateStore`; do not branch on backend inside
  `RouteStatePromotionService`.
- Add backend-name selection only in `RouteStateStoreFactory` and ops catalog
  composition.
- Add new blocker rules in `RouteStatePromotionPolicy` with tests and runbook
  docs in the same PR.
- Keep report fields additive. Do not rename existing JSON keys.
- Keep CLI handlers thin: parse arguments, call service, emit report.

## Tests

Focused tests live in:

- `tests/test_route_state_promotion.py`
- `tests/test_route_state_promotion_sqlite.py`
- `tests/test_cli_route_state_promotion_command.py`
- `tests/test_route_state_promotion_docs_contract.py`

Run:

```bash
uv run pytest tests/test_route_state_promotion.py tests/test_route_state_promotion_sqlite.py tests/test_cli_route_state_promotion_command.py tests/test_route_state_promotion_docs_contract.py -q
uv run ruff check src/dpone/ops/routes/state_promotion_models.py src/dpone/ops/routes/state_promotion_policy.py src/dpone/ops/routes/state_store.py src/dpone/ops/routes/state_store_sqlite.py src/dpone/ops/routes/state_store_factory.py src/dpone/ops/route_state_promotion.py
```
