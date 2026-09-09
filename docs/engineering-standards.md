# Engineering standards

Status: normative
Applies to: handwritten production code, public contracts, tests, tooling, and
architecture changes.

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** define decreasing levels
of obligation. A deviation from a MUST requires an approved issue or ADR with a
rationale, owner, review date, and remediation or retirement plan.

## Decision priority

Engineering decisions MUST prioritize, in order:

1. prevention of silent loss, duplication, corruption, and false success;
2. deterministic public behavior and backward compatibility;
3. security, auditability, recoverability, and operability;
4. understandable user experience;
5. maintainability and extensibility;
6. performance and implementation convenience, after the preceding properties
   are protected.

## Operational meaning of the design principles

### Single responsibility

A module or class MUST have one coherent reason to change. CLI parsing, planning,
connector I/O, state transitions, evidence generation, and rendering are separate
responsibilities unless a very small cohesive component proves otherwise.

### Open/closed and capability design

New connectors and strategies SHOULD extend explicit capabilities, ports, and
registries. Do not add growing vendor `if/elif` chains to domain policy. A new
extension point needs at least one real variation point and a clear lifecycle;
one implementation alone does not justify a generic plugin framework.

### Liskov substitution

Implementations of a port MUST preserve its documented input, output, error,
ordering, idempotency, and side-effect contracts. A subtype that silently ignores
an unsupported option or weakens evidence is not substitutable.

### Interface segregation

Interfaces MUST be capability-oriented and minimal. Consumers must not depend on
methods they do not use. Prefer separate read, write, state, schema, evidence, and
transaction capabilities to a universal connector interface.

### Dependency inversion and DI

Domain and application policy depend on ports, not vendor SDKs or framework
objects. Concrete clients, clocks, state stores, artifact writers, and policies
are constructed in explicit composition roots and passed in. Service locators,
hidden global clients, and import-time environment discovery are prohibited.

### DRY

Deduplicate knowledge, contracts, schemas, and algorithms. Similar syntax is not
sufficient evidence of duplicated knowledge. Apply the rule of three before
introducing an abstraction unless a public invariant already demands one.

### KISS

Prefer the smallest design that fully expresses the required contracts and
failure semantics. KISS does not justify omitting retries, state ordering,
validation, observability, or migration behavior.

## Module and class design

- Adapters and CLI commands SHOULD translate inputs/outputs and delegate policy.
- Domain policy MUST NOT import Airflow, dbt, Typer/Click, or vendor SDK types.
- Compatibility modules MUST adapt or re-export only.
- A class SHOULD own an invariant, lifecycle, resource, or polymorphic behavior.
  Do not create classes solely to wrap one stateless function.
- Avoid boolean-flag APIs that create hidden modes; use explicit policy or
  strategy objects when behavior genuinely varies.
- Avoid god modules, god services, deep inheritance, cyclic dependencies, and
  mutable global registries.
- Split code by responsibility and stable variation point, not arbitrary line
  chunks. `part_1.py`/`part_2.py` decomposition is not acceptable.

## Quality budgets as code

`docs/benchmarks/quality_budgets.yml` is the single source of truth. At the time
of this standard it contains global hard budgets including:

```yaml
max_sloc: 400
max_avg_clustering: 0.180
```

The checked-in file, not copied prose, governs when values change.

- New handwritten production modules MUST comply.
- Existing debt MUST NOT increase.
- Generated code is excluded only when its producer and generated status are
  explicit and tested.
- A file below 400 SLOC can still violate cohesion; metrics are guardrails, not
  substitutes for design review.
- The clustering metric MUST use the repository implementation and its defined
  graph model. Agents must not substitute another same-named metric.

Required checks include:

```bash
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"
BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"
if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size \
  --baseline docs/module_size_baseline.json \
  --base-ref "$BASE_SHA" \
  --head-ref "$HEAD_SHA"
uv run dpone docs check-architecture-fitness
```

## Public contract discipline

The following are public contracts unless explicitly documented otherwise:

- manifest and JSON schema shape, defaults, and validation;
- CLI command names, options, exit codes, stdout/stderr, JSON/file output;
- Python imports, call signatures, return models, and documented exceptions;
- connector capabilities and supported route/strategy combinations;
- row, parent, root, schema, run, and artifact identity;
- checkpoint/state ordering, retry, replay, and idempotency;
- evidence schemas, artifact names, status vocabulary, and digests;
- Airflow/dbt integration behavior and documented configuration.

A contract change MUST include compatibility analysis, tests for old and new
behavior, documentation, migration guidance, changelog entry, and an ADR when it
changes architecture or long-lived semantics.

## Failure and evidence semantics

- Validate unsupported combinations before durable side effects.
- Fail closed when capability, identity, state, or evidence is ambiguous.
- State/checkpoint promotion MUST occur only after the data and evidence boundary
  required by the contract has succeeded.
- Retries and replay MUST be deterministic and idempotent or clearly document the
  deduplication key and operator action.
- Evidence MUST describe what actually ran. `SKIP`, mocked, stale, and unavailable
  are not aliases for `PASS`.

## Exception process

An exception records:

```yaml
rule:
reason:
owner:
scope:
introduced_at:
review_by:
risk:
mitigation:
remediation_plan:
```

Permanent silent exceptions are prohibited.
