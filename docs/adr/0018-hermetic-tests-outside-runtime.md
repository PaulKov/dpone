# ADR 0018: Hermetic tests stay outside the production runtime

## Status

Accepted.

## Context

The frozen Airflow self-service roadmap requires executable `dpone.test.v1`
fixtures, deterministic temporary targets, and CI summaries. dpone already has
one canonical compiler and a production runtime with connectors, credentials,
state, evidence, schema evolution, transports, and vendor-specific strategies.

Calling that runtime from a supposed hermetic test would make network and secret
absence difficult to prove, couple tests to installed connector SDKs, and make a
local pass look like route certification. Building a second manifest compiler
or copying sink implementations into the test lane would instead create policy
drift.

## Decision

dpone defines a separate build/test-plane execution boundary:

- `dpone.test.v1` always compiles its pipeline through the canonical
  `AuthoringCompiler`.
- A narrow `HermeticStrategyExecutor` port accepts already normalized process
  policy and immutable fixture rows.
- The default in-memory adapter implements only connector-neutral successful
  final-state semantics for `full_refresh`, `incremental_append`, and
  `incremental_merge` with duplicate-policy `fail`.
- SQL/Python transforms, source query files, automatic strategy negotiation,
  CDC, backfill, DLQ, schema evolution, reconciliation, and vendor behavior fail
  closed until a separately approved hermetic contract exists.
- The service imports no production source/sink connector, Airflow, Vault,
  Kubernetes, vendor SDK, or credential resolver.
- Every report labels coverage `hermetic_contract` and lists connector, vendor
  SQL, transport, physical DDL, and credentials as excluded.
- The temporary target is an immutable in-memory value identified from test
  content. It is never a database or durable state/checkpoint.
- Reports are safe metadata, not production run evidence or certification.

## Consequences

- A fresh scaffold can be tested quickly without infrastructure or secrets.
- Static check, preview, build, test, and loading share authoring validity.
- Deterministic tests can catch contract and strategy expectation regressions,
  but cannot prove that a live route works.
- Unsupported behavior is visible early rather than silently approximated.
- The port preserves dependency inversion without introducing a general test
  plugin registry.
- Connector and route certification remain distinct release gates.

## Alternatives rejected

- Hydrate the real runtime with fake connectors: too much runtime/secret/import
  surface and ambiguous certification meaning.
- Use SQLite or DuckDB as a universal sink: adds translation and vendor-semantic
  drift.
- Implement one test executor per connector: duplicates production strategy
  policy and defeats hermetic portability.
- Validate only schemas/counts: insufficient to test load-strategy semantics.
- Permit best-effort execution of unknown features: risks false success.

## References

- [Feature design: hermetic pipeline tests v1](../feature-design-airflow-hermetic-tests-v1.md)
- [ADR 0004: Runtime ports and bootstrap](0004-runtime-ports-and-bootstrap.md)
- [ADR 0006: Authority and canonical IR](0006-authority-and-canonical-ir.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0015: One authoring compiler](0015-single-authoring-compiler.md)
- [Airflow self-service backlog](../airflow-self-service-backlog.md)
