# ADR 0002: Variant C batch manifest

Status: accepted

## Context

Batch pipelines need reusable defaults, environment-aware registry entries, and per-table overrides without turning manifests into imperative code.

## Decision

Adopt Variant C manifests with layered resolution:

1. Registry defaults.
2. Convention defaults.
3. File-level variables.
4. File-level defaults.
5. Table-level configuration.

The resolved manifest is deterministic and can be explained by the CLI.

## Consequences

- Manifests stay compact but explicit.
- Common connection and naming policies are reusable.
- `manifest explain` can show provenance for resolved values.
- Runtime execution receives a fully resolved contract, not a partially interpreted file.

## Related docs

- [Manifests](../manifests-variant-c.md)
- [Overrides](../overrides.md)
- [Registry](../registry.md)
