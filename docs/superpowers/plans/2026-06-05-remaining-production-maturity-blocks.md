# Remaining Production Maturity Blocks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the remaining production maturity layers for dpone: CDC replay/idempotency, object storage staging, supply-chain attestations, and Connector SDK certification.

**Architecture:** Keep each domain in a focused package with a thin CLI adapter and explicit docs/runbooks. CDC replay remains a credential-free control-plane planner, object storage staging uses injected clients, supply-chain evidence is dependency-light and deterministic, and connector SDK scaffolding generates testable community connector packages.

**Tech Stack:** Python 3.11+, argparse CLI adapters, pytest, MkDocs, GitHub Actions, SHA-256 checksums, SPDX/CycloneDX-like JSON, in-toto/SLSA-inspired provenance, S3/GCS/Azure/local object storage adapters.

---

## File ownership map

| Block | Runtime files | CLI/docs/tests |
| --- | --- | --- |
| CDC replay and idempotency | `src/dpone/readiness/cdc_replay.py`, `src/dpone/runtime/cdc/identity.py`, `src/dpone/runtime/cdc/replay.py`, `src/dpone/services/cdc_replay.py` | `src/dpone/commands/cdc_replay_cmd.py`, `docs/cdc.md`, `docs/developer-cdc.md`, `tests/test_runtime_cdc_replay.py`, `tests/test_cli_cdc_replay_command.py` |
| Object storage staging | `src/dpone/storage/*`, `src/dpone/staging/object_storage.py` | `docs/object-storage-staging.md`, `docs/developer-object-storage.md`, `tests/test_object_storage_staging.py` |
| Supply-chain evidence | `src/dpone/supply_chain/*` | `src/dpone/commands/supply_chain_cmd.py`, `docs/supply-chain.md`, `docs/developer-supply-chain.md`, `tests/test_supply_chain_attestation.py`, `tests/test_cli_supply_chain_commands.py` |
| Connector SDK | `src/dpone/connector_sdk/*` | `src/dpone/commands/connectors_cmd.py`, `docs/connector-sdk.md`, `docs/developer-connector-sdk.md`, `tests/test_connector_sdk_scaffold.py`, `tests/test_cli_connector_sdk_commands.py` |

## Task 1: CDC replay/idempotency hardening

- [x] Add deterministic event identity and duplicate detection.
- [x] Add replay planner blockers for unsafe rewinds, retention gaps, high-watermark overflow, and artifact-backed replay.
- [x] Add commit gate requiring sink success, committed load state, matching backend, and idempotency pass.
- [x] Expose `dpone cdc replay-plan`.
- [x] Document replay runbooks and developer extension points.
- [x] Verify with `uv run pytest tests/test_runtime_cdc_replay.py tests/test_cli_cdc_replay_command.py -q`.

## Task 2: Object storage staging

- [x] Add provider-neutral `ObjectStorageUri` and injectable storage clients for S3, GCS, Azure Blob, and local CI.
- [x] Add staging plan/manifest/service with SHA-256 evidence and cleanup policy.
- [x] Document URI forms, staging flow, cleanup policy, and runbooks.
- [x] Verify with `uv run pytest tests/test_object_storage_staging.py -q`.

## Task 3: Supply-chain evidence

- [x] Add SBOM generation for SPDX-like and CycloneDX-like JSON.
- [x] Add provenance generation for release artifacts.
- [x] Add local HMAC signature envelopes for deterministic CI evidence.
- [x] Expose supply-chain CLI commands.
- [x] Document Trusted Publishing/Sigstore/GitHub attestation positioning and local fallback.
- [x] Verify with `uv run pytest tests/test_supply_chain_attestation.py tests/test_cli_supply_chain_commands.py -q`.

## Task 4: Connector SDK and generated certification suite

- [x] Add connector scaffold service with package, docs, examples, tests, and certification manifest.
- [x] Add certification template service scoped by source/sink/state capabilities.
- [x] Expose `dpone connectors scaffold`.
- [x] Document user and developer workflows for community connectors.
- [x] Verify with `uv run pytest tests/test_connector_sdk_scaffold.py tests/test_cli_connector_sdk_commands.py tests/test_connector_sdk_docs_contract.py -q`.

## Task 5: Final gates

- [ ] Run `uv run ruff check .`.
- [ ] Run `uv run ruff format --check .`.
- [ ] Run `uv run mypy --config-file mypy.ini`.
- [ ] Run `uv run dpone docs check-docs`.
- [ ] Run `uv run mkdocs build --strict`.
- [ ] Run `uv run pytest -m "not integration_live" -q`.
- [ ] Run `uv build`.
- [ ] Run `uv tool run twine check dist/*`.
