# OOP refactoring plan

This document records the historical refactoring direction and current status.

## Goal

The codebase should be:

- More modular.
- Easier to read.
- Easier to extend.
- Protected by explicit architecture boundaries and quality gates.

## Completed work

Major milestones:

- Introduced `AppContext` and a composition root.
- Split CLI implementation into `commands/*`.
- Moved DAG logic into `dpone.dag.*`.
- Moved runtime logic into `dpone.runtime.*`.
- Removed direct dependencies from manifest/DAG commands to legacy CLI internals.
- Introduced `cli_render/*` and typed view models.
- Cleaned manifest/DAG to runtime boundaries.
- Split large DAG modules such as config, manager, graph, and edge resolver.
- Split large runtime modules such as ETL processor, database strategies, staging, and ClickHouse connector.
- Added import-rule and layer-metrics gates.
- Reduced runtime-to-core/library coupling.

## Current status

The refactoring is effectively complete for the OSS MVP architecture. Remaining work should focus on incremental hardening rather than large structural rewrites.

## Remaining work

1. Keep documentation aligned with current architecture.
2. Review transitional compatibility shims before each minor release.
3. Tighten quality thresholds only when the team can support the new baseline.

## Implementation principle

Refactoring should happen in small, backward-compatible steps. Compatibility shims should be thin, documented, and scheduled for removal only after consumers have a migration path.
