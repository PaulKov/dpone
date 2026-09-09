# ADR 0001: Layered architecture

Status: accepted

## Context

`dpone` contains CLI commands, manifest parsing, DAG rendering, runtime execution, connectors, and developer tooling. Without explicit layer boundaries, optional dependencies and provider-specific code can leak into lightweight paths such as `dpone --help` or manifest validation.

## Decision

Use a layered architecture:

1. Contracts and configuration models.
2. Manifest parsing and dependency resolution.
3. Runtime service interfaces.
4. Connector implementations.
5. CLI and rendering adapters.

Higher layers may depend on lower layers. Lower layers must not import runtime-heavy adapters or optional provider SDKs.

## Consequences

- Optional dependencies remain lazy.
- CLI help and manifest validation stay fast and install-light.
- Connector code can evolve without breaking manifest tooling.
- Import rules and layer metrics can be checked in CI.

## Guardrails

- Keep heavy imports inside execution paths.
- Add connector-specific dependencies behind extras.
- Update import-rule tests when a new public layer is introduced.
- Use the runtime port interfaces instead of calling connector internals from the CLI.
