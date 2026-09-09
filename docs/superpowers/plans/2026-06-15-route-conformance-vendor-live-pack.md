# Route Conformance Vendor-Live Pack Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in vendor-live Route Conformance Pack that verifies real Postgres, MSSQL, and ClickHouse endpoints with the same 10,000 row, 200 column, schema-evolution, and exact typed-hash contract used by offline conformance.

**Architecture:** Keep `RouteConformanceLiveService` as a facade. Add a generic schema-evolution live port, a blocked-step report path, and a route-binding adapter that composes source/sink stores. Keep database-specific SQL in small injected store/dialect collaborators so future routes add bindings and stores, not CLI logic.

**Tech Stack:** Python dataclasses/protocols, existing dpone ops route taxonomy, existing runtime connectors loaded lazily, pytest, ruff, mypy, MkDocs.

---

### Task 1: Red Tests And Plan

**Files:**
- Create: `docs/superpowers/plans/2026-06-15-route-conformance-vendor-live-pack.md`
- Modify: `tests/test_route_conformance_live.py`
- Modify: `tests/test_cli_route_conformance_commands.py`
- Modify: `tests/test_route_conformance_docs_contract.py`

- [ ] **Step 1: Add failing unit tests**

Add tests proving:

```python
def test_vendor_live_registry_exposes_opt_in_adapters() -> None:
    from dpone.ops.routes.conformance_live_adapters import default_live_adapter_registry

    registry = default_live_adapter_registry()

    assert "in_memory" in registry
    assert "vendor_live" in registry
    assert "docker" in registry
```

Add tests proving a blocked live step short-circuits before snapshots:

```python
class _BlockedAdapter:
    def seed_source(self, *, route, dataset, config):
        return RouteConformanceLiveStep(
            name="seed_source",
            status="blocked",
            summary="opt-in missing",
            blockers=("vendor_live.opt_in_missing",),
        )

    def apply_schema_evolution(self, *, route, dataset, config):
        raise AssertionError("schema evolution should not run after a blocked seed")

    def execute_route(self, *, route, dataset, config):
        raise AssertionError("route execution should not run after a blocked seed")

    def read_source_snapshot(self, *, route, dataset, config):
        raise AssertionError("snapshots should not be read after a blocked seed")

    def read_sink_snapshot(self, *, route, dataset, config):
        raise AssertionError("snapshots should not be read after a blocked seed")
```

Add tests proving a memory-backed vendor adapter can run both first routes through route bindings:

```python
report = RouteConformanceLiveService(
    adapter_registry={"vendor_live": adapter}
).run(
    output_dir=tmp_path / "vendor",
    source="postgres",
    sink="mssql",
    strategy="incremental_merge",
    config=RouteConformanceLiveConfig(
        adapter="vendor_live",
        dataset=RouteConformanceDatasetProfile(
            name="wide_vendor_contract",
            row_count=128,
            column_count=32,
            chunk_size=32,
            include_nested=True,
            include_schema_evolution=True,
        ),
        min_rows=100,
        min_columns=32,
        require_schema_evolution=True,
    ),
)
assert report.passed is True
```

- [ ] **Step 2: Verify red**

Run:

```bash
uv run pytest tests/test_route_conformance_live.py tests/test_cli_route_conformance_commands.py tests/test_route_conformance_docs_contract.py -q
```

Expected: failures because the vendor-live adapter, schema-evolution live port, blocked-step short-circuit, and docs strings do not exist yet.

### Task 2: Live Service Port And Blocked Step Handling

**Files:**
- Modify: `src/dpone/ops/routes/conformance_live_ports.py`
- Modify: `src/dpone/ops/route_conformance_live.py`
- Modify: `src/dpone/ops/routes/conformance_live_adapters.py`
- Test: `tests/test_route_conformance_live.py`

- [ ] **Step 1: Add schema-evolution live port**

Add `RouteConformanceSchemaEvolutionApplier`:

```python
class RouteConformanceSchemaEvolutionApplier(Protocol):
    def apply_schema_evolution(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep: ...
```

Make `RouteConformanceLiveAdapter` inherit it.

- [ ] **Step 2: Add generic blocked-step report helper**

Add a service helper that writes `route_conformance_live.json` when any adapter step returns blockers:

```python
def _blocked_step_report(..., steps: Sequence[RouteConformanceLiveStep]) -> RouteConformanceLiveReport:
    blockers = tuple(dict.fromkeys(blocker for step in steps for blocker in step.blockers))
    decision = RouteConformanceDecision(
        passed=False,
        status="blocked",
        score=0.0,
        blockers=blockers,
        warnings=tuple(),
        next_actions=("Fix the blocked live adapter step, then rerun live-run.",),
    )
```

- [ ] **Step 3: Verify green**

Run:

```bash
uv run pytest tests/test_route_conformance_live.py -q
```

Expected: new blocked-step tests pass while vendor-live registry tests may still fail until Task 3.

### Task 3: Vendor-Live Route Binding Adapter

**Files:**
- Create: `src/dpone/ops/routes/conformance_vendor_live.py`
- Modify: `src/dpone/ops/routes/conformance_live_adapters.py`
- Modify: `src/dpone/ops/routes/__init__.py`
- Test: `tests/test_route_conformance_live.py`

- [ ] **Step 1: Add store protocols and in-memory store**

Add small contracts:

```python
class RouteConformanceLiveStore(Protocol):
    backend: str
    def seed(self, dataset_id: str, columns: Sequence[RouteConformanceColumn], rows: Sequence[Mapping[str, object]]) -> int: ...
    def reset(self, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None: ...
    def replace_rows(self, dataset_id: str, columns: Sequence[RouteConformanceColumn], rows: Sequence[Mapping[str, object]]) -> int: ...
    def add_column(self, dataset_id: str, column: RouteConformanceColumn, default_value: object) -> None: ...
    def read_snapshot(self, dataset_id: str, kind: str, columns: Sequence[RouteConformanceColumn]) -> RouteConformanceSnapshot: ...
```

Add `MemoryRouteConformanceLiveStore` for deterministic local tests.

- [ ] **Step 2: Add route-binding adapter**

Add `VendorRouteConformanceLiveAdapter` that owns bindings:

```python
@dataclass(frozen=True, slots=True)
class VendorLiveRouteBinding:
    source: str
    sink: str
    strategy: str
    source_store: RouteConformanceLiveStore
    sink_store: RouteConformanceLiveStore
```

The adapter:
- fails closed unless `DPONE_VENDOR_LIVE=1` when opt-in is required;
- seeds the source and resets the sink;
- applies an evolved decimal column when required;
- copies current source rows to sink through the store port;
- reads snapshots from both stores.

- [ ] **Step 3: Add lazy SQL store factories**

Add `DockerVendorLiveRouteConformanceAdapterFactory.from_env()` that creates Postgres, MSSQL, and ClickHouse stores lazily from `DPONE_IT_*` environment variables. Import optional runtime connectors only inside factory methods.

- [ ] **Step 4: Verify green**

Run:

```bash
uv run pytest tests/test_route_conformance_live.py -q
```

Expected: all route conformance live tests pass.

### Task 4: CLI, Docs, Runbooks

**Files:**
- Modify: `src/dpone/commands/ops_parsers_route_conformance.py`
- Modify: `docs/route-conformance-lab.md`
- Modify: `docs/developer-route-conformance-lab.md`
- Modify: `docs/architecture.md`
- Modify: `docs/developer-ci-cd.md`
- Modify: `docs/source-sink-matrix.md`
- Modify: `tests/test_cli_route_conformance_commands.py`
- Modify: `tests/test_route_conformance_docs_contract.py`

- [ ] **Step 1: Add CLI examples and docs contract coverage**

Document:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql clickhouse
DPONE_VENDOR_LIVE=1 DPONE_RUN_INTEGRATION=1 \
uv run dpone ops route-conformance live-run \
  --adapter vendor_live \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --rows 10000 \
  --columns 200 \
  --require-schema-evolution
```

Add docs contract assertions for `DPONE_VENDOR_LIVE=1`, `--adapter vendor_live`, `--adapter docker`, and both first routes.

- [ ] **Step 2: Verify docs tests**

Run:

```bash
uv run pytest tests/test_cli_route_conformance_commands.py tests/test_route_conformance_docs_contract.py -q
```

Expected: pass.

### Task 5: Full Verification, Commit, Push, PR Update

**Files:**
- All changed files.

- [ ] **Step 1: Run focused gates**

```bash
uv run pytest tests/test_route_conformance_lab.py tests/test_route_conformance_live.py tests/test_cli_route_conformance_commands.py tests/test_route_conformance_docs_contract.py -q
```

- [ ] **Step 2: Run full quality gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -q
uv run dpone docs update-cli-reference --check
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-architecture-fitness
uv run mkdocs build --strict
```

- [ ] **Step 3: Commit and publish**

Stage only intended tracked/new files, commit with:

```bash
git commit -m "Add vendor live route conformance pack"
git push
```

Update PR #76 with a comment summarizing the vendor-live pack and verification results.
