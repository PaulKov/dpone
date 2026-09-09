# ADR 0017: Canonical workload selection algebra

## Status

Accepted.

## Context

The Airflow self-service roadmap requires tag/domain/owner/source/sink/group
selectors, graph expansion, named selectors, semantic state comparison, and an
explanation of every selected node. dpone already has process DAG traversal,
GitOps changed-file impact, workload catalogs, and Airflow DAG spec edges, but
none of those is the public project-selection authority. Reusing one of them
directly would either select the wrong entity (`Step`/process instead of
`Workload`) or couple selection to a compatibility adapter.

Resolving a mutable selector during Airflow DAG parsing would also violate ADR
0008: the provider must consume a bounded static deployment index without
repository discovery, YAML compilation, state reads, network, Variables,
Connections, or secrets.

## Decision

dpone defines one orchestrator-neutral workload selection algebra on the
CLI/build plane.

- `Workload` is the selector node because it is the minimum independently
  runnable dpone unit.
- A pure immutable `SelectionGraph` contains normalized workload metadata and
  explicit directed workload edges. It is independent of filesystem, CLI,
  Airflow, Git, connectors, and credential services.
- One `SelectionEngine` owns exact method matching, repeated-select union,
  exclude-after-expansion subtraction, prefix/suffix `+` traversal, named
  selector expansion, deterministic reasons, and selection fingerprints.
- Project adapters build the graph from bounded domain catalogs, explicit
  primary authoring sources, the canonical authoring compiler, explicit
  workload dependencies, and DAG wiring.
- Asset-inferred edges may be added in Phase 3 only as another provenance-
  retaining graph adapter. They do not create a second selector engine.
- `state:*` compares canonical semantic and graph fingerprints against one
  explicit content-fingerprinted local baseline. Missing state fails closed.
- Selection is fully resolved before release materialization. Check, preview,
  and safe sample planning consume the same immutable report.
- Airflow receives only selected static DAG specs/packs in the ordinary release
  and deployment index. The provider never imports selector code.
- v1 deliberately excludes the full dbt grammar: no wildcard, comma
  intersection, `@`, quantified `N+`, regex, Jinja, or arbitrary Boolean
  expressions.
- Existing `dpone run --selector` remains the process selector for one manifest;
  project `--select` is a separate additive contract.

## Consequences

- A selected set and its reasons are deterministic across check, preview, and
  safe sample planning.
- Airflow parse performance and security stay independent of project size and
  selector syntax.
- The project loader is a real application adapter with explicit budgets; it is
  not hidden recursive discovery.
- State comparison detects semantic and graph changes rather than source
  formatting changes.
- v1 is intentionally less expressive than dbt. More operators require an
  approved grammar/version change rather than accidental parser growth.
- Existing GitOps impact and process graph services remain valid for their own
  responsibilities but cannot implement public workload selection policy.

## Alternatives rejected

- Extend `AffectedWorkloadResolver` into the public selector engine: it maps
  changed paths and has no canonical graph/set/state semantics.
- Select process nodes in `dpone.dag.DependencyGraph`: a process is not always
  independently runnable and may be inline inside one workload.
- Implement selectors in each command: violates DRY and produces parity drift.
- Parse selectors in `load_dpone_dags`: violates the parse-safe provider
  contract and reproducible topology.
- Adopt all dbt syntax: excessive grammar and compatibility surface for the v1
  user journey.

## References

- [Feature design: explainable project selectors v1](../feature-design-airflow-selectors-v1.md)
- [ADR 0006: Authority and canonical IR](0006-authority-and-canonical-ir.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0015: One authoring compiler](0015-single-authoring-compiler.md)
- [dbt node selection syntax](https://docs.getdbt.com/reference/node-selection/syntax)
- [Astronomer Cosmos selecting and excluding](https://astronomer.github.io/astronomer-cosmos/guides/translate_dbt_to_airflow/selecting-excluding.html)
