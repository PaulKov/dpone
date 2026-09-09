# Production-code rules

These rules extend the repository-level `AGENTS.md` for `src/dpone/**`.

- Preserve the dependency direction defined in `docs/architecture.md` and
  `docs/import-rules.md`. Domain contracts and policies must not depend on CLI,
  Airflow, dbt, or vendor adapters.
- Use canonical imports. Compatibility modules may translate or re-export but
  must not become a second implementation.
- Keep public imports lazy. `import dpone` and `dpone --help` must not require
  optional connector SDKs or perform network, filesystem, credential, or
  database I/O.
- Put construction in explicit composition roots. Pass clocks, state stores,
  clients, capability providers, artifact writers, and policies through narrow
  interfaces rather than globals.
- Treat row identity, parent identity, schema identity, state/checkpoint order,
  evidence order, idempotency, and retry behavior as correctness invariants.
- A new connector or strategy must declare capabilities and fail closed for
  unsupported combinations. Do not infer support from method presence alone.
- A public behavior change requires tests, docs/examples, compatibility impact,
  and migration guidance. Architectural decisions require an ADR.
- Follow `docs/benchmarks/quality_budgets.yml`; existing module-size or coupling
  debt must not increase.
