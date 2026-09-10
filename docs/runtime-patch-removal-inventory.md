# Runtime patch removal inventory

This inventory is for maintainers replacing runtime method/global mutation with
explicit dependency injection. It records source inspection of v0.74.36,
`3977ca2d04ca5dbcf3338d7c31faff8a1199549e`, on 2026-09-10. It is a removal plan,
not a statement that all listed mechanisms have been removed.

See [agent development](agent-development.md) for implementation/review rules and
the [bounded MSSQL design](feature-design-clickhouse-mssql-bounded-native-v1.md)
for the associated transport work. Production examples and evidence must remain
synthetic and must not contain deployment-specific identifiers or credentials.

The credential, logging and Airflow replacements are implemented for review in
[PR #12](https://github.com/PaulKov/dpone/pull/12). This records proposed removal
on that branch, not absence on released v0.74.36. The native transport remains a
separate researched contract awaiting maintainer approval.

## Active inventory and removal order

| Order | Patch and purpose | Explicit replacement | Migration and regression evidence |
|---|---|---|---|
| 1 | `runtime/connectors/api/appsflyer.py`: temporarily replaces canonical credential helper functions and credential class while loading credentials | Canonical loader with injected environment resolver, manager factory and credential factory; facade delegates without modifying the canonical module | Preserve credential/connector construction results and defaults. Add concurrent-call isolation and failure tests to `test_runtime_appsflyer_connector.py`; migrate customization to explicit arguments. Remove mutation code, including restore-in-finally paths. |
| 2 | `dpone_airflow_pack/live_base_logs.py`: replaces `read_pod_logs` on an instance to classify transient log transport errors | Owned PodManager adapter/subclass supplied by the operator's manager construction seam | Preserve fail-closed non-log errors, fallback polling, XCom, final wait and cleanup in `test_kpo_live_base_container_logs.py`. Remove dynamic method assignment after adapter tests pass. |
| 2 | Same module replaces `_refresh_cached_properties` to reapply the wrapper after provider credential refresh | Explicit refresh-aware construction in the owned operator subclass | Test provider manager recreation and supported signatures. Do not preserve a compatibility path that reinstalls wrappers. |
| 3 | `runtime/logging_core.py` and `runtime/etl_logging/etl_logger.py` set parent-module exports at import | Public facade owns ordinary exports; injected `RuntimeLogger` remains the runtime dependency | Test package-first, submodule-first and runtime-facade-first import identities in fresh processes. The submodule and singleton share a name; avoid exposing the submodule as the singleton. Remove cross-module synchronization helpers. |

The AppsFlyer mutation can interleave across calls and restore another caller's
globals. The Airflow mutations are scoped instance modifications, not import-time
class changes. Logging synchronization changes module exports, not class methods.
All three require targeted replacement rather than blanket deletion of `setattr`.

## Adjacent compatibility mechanisms

- `readiness/airflow_runtime_pack_exec.py` detects a patched `subprocess.run` and
  selects a historical test path. Replace this detection with an injected process
  runner while preserving streaming stderr and exit handling.
- `commands/resume_cmd.py` and `resync_cmd.py` consult `sys.modules` for replay
  support. They do not install a replacement, but explicit service injection is a
  clearer seam. Preserve CLI registration and replay backend tests.
- `runtime/credentials/connector_factory.py` caches a manager on a class at
  explicit use. This is shared service state, not a method patch. Record it as
  separate dependency-injection debt; do not call its presence an import patch.
- Own-module lazy export caching, artifact metadata attachment and exception
  details are ordinary data/export operations. Do not remove them on a keyword
  match alone.

## Bootstrap and package findings

Targeted source searches and independent AST review found no owned production
`sitecustomize.py`, `usercustomize.py`, `.pth`, `*_patched` source artifact, or
production assignment into `sys.modules` under `src` and `packages`. This is a
scoped source observation, not proof about external installed environments or
arbitrary dynamic Python behavior.

Doctor tests deliberately create startup-hook fixtures. Keep those tests and the
runtime diagnostic code that rejects unsupported startup customization. The
runtime Docker entry point uses the installed `dpone` command. Authenticated dbt
configuration overlays do not install Python monkey patches. Benchmark tripwires
use explicit test instrumentation and are not production bootstrap mechanisms.

## Regression gates

The metadata-only `tools/agent_policy/package_archive_gate.py` rejects Python
startup hook members in wheel and sdist: `.pth`, customizer modules, compiled
customizer variants and customizer packages, including nested installation paths.
Its existing build-workflow callers cover package artifacts. It does not inspect
or execute archive payloads; ordinary documentation mentioning hooks is allowed.

Run after building all maintained Python distributions:

```bash
uv run python tools/agent_policy/package_archive_gate.py dist/*.whl dist/*.tar.gz
```

A separate source gate remains to be implemented alongside runtime removal. It
must detect class/module/function replacement in import-executed statements,
simple aliases and directly called initialization helpers; reject unresolved
dynamic installation with an explicit finding; and cover all production package
roots. Document analysis limits instead of claiming a general proof about Python.
Instance data injection, subclass definitions and own-module re-exports must pass.

## Review and release boundaries

Use independently reviewable changes for canonical extension points, migrated call
sites, then physical removal and source-gate enforcement. Each deletion follows
regression coverage of the original supported behavior. No deprecated alias may
continue executing a patch. A fresh-context reviewer checks each implementation.
The inventory and archive rule alone do not complete the removal campaign or the
bounded transport. Merge, live route certification and release remain separate.
