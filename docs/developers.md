# Developers guide

## Quick navigation

- [Architecture](architecture.md)
- [CLI reference](cli-reference.md)
- [CLI examples](cli-examples.md)
- [Variant C manifests](manifests-variant-c.md)
- [Developer manifest sparse paths](developer-manifest-sparse-paths.md)
- [Developer GitOps control plane](developer-gitops-control-plane.md)
- [Overrides](overrides.md)
- [Conventions](conventions.md)
- [Registry](registry.md)
- [DAG debugging](dag-debugging.md)
- [Import rules](import-rules.md)
- [Quality metrics](quality-metrics.md)
- [Developer workflow](developer-workflow.md)
- [CI/CD](ci-cd.md)
- [Developer CI/CD guide](developer-ci-cd.md)
- [Runbook](runbook.md)
- [Release](release.md)
- [Compatibility](compatibility.md)
- [ADR index](adr-index.md)

## Repository layout

This repository uses **src-layout**:

- Python package code is under `src/dpone/`
- Documentation is under `docs/`

## Target refactoring structure

We are moving from a monolithic CLI module to a layered structure:

- `dpone/cli/` – thin entrypoint
- `dpone/commands/` – argparse wiring + command dispatch
- `dpone/services/` – application services (use cases)
- `dpone/app/` – DI / settings / logging
- `dpone/ports/` – Protocols
- `dpone/adapters/` – concrete implementations

## Quality gates and metrics

Code quality metrics are maintained in [Quality metrics](quality-metrics.md). That page is the single source of truth for current LOC/SLOC, coupling, clustering, layer metrics, thresholds, and CI update commands.

Use the developer guide for workflow and architecture orientation; use the quality dashboard when deciding whether a refactor is required before adding new behavior.
