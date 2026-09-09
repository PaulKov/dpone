# Route Schema Evolution and Repair Design

## Goal

Add a reusable route-level control-plane capability for schema evolution and
reconciliation repair evidence across any `source -> sink -> strategy` route.

## Scope

This feature does not replace runtime connectors and does not run heavy live
database tests. It composes existing CDC schema evolution, schema apply,
reconciliation, and route readiness artifacts into a stable route contract for
CLI, CI, docs, and release gates.

## Architecture

The route taxonomy remains the entry point. `RouteKey` identifies the route,
`RouteProfileCatalog` proves the route exists in the integration matrix, and
small route services build stable reports. Route-specific behavior must come
from route profiles, policy inputs, or upstream artifacts.

New route-level boundaries:

- `dpone.ops.routes.models`: public value objects and report contracts.
- `dpone.ops.routes.schema_evolution`: route schema evolution service.
- `dpone.ops.routes.reconciliation_repair`: route reconciliation repair service.
- `dpone.ops.routes.policy`: route-level go/no-go helpers.
- `dpone.ops.route_schema_evolution`: small facade for CLI/service catalog.
- `dpone.ops.route_reconciliation_repair`: small facade for CLI/service catalog.

## Data Flow

`route-schema-evolution` accepts a route identity and a CDC schema evolution
artifact. It validates route/profile consistency, classifies safe versus unsafe
schema changes, emits a DDL/apply decision, and writes JSON/Markdown artifacts.

`route-reconciliation-repair` accepts a route identity plus source/target rows
or an existing reconciliation artifact. It delegates row comparison semantics to
the existing reconciliation service where possible, classifies repair actions,
and writes a route repair receipt.

Both reports can be attached to `route-readiness` and `route-release-gate` as
normal evidence artifacts.

## Error Handling

Unsupported routes fail closed with `route.unsupported:<colon_id>`. Route
mismatches between CLI arguments and upstream artifacts are blockers. Missing,
invalid, unsafe, or unapproved schema changes are blockers. Reconciliation
differences generate repair actions and block release promotion until repaired
or explicitly accepted by a higher-level policy.

## Testing

Tests are written first. Coverage must include generic route behavior,
`mssql -> clickhouse`, `postgres -> mssql`, schema-change pass/fail cases,
repair action classification, CLI JSON/Markdown behavior, docs contracts, and
generated CLI reference checks.
