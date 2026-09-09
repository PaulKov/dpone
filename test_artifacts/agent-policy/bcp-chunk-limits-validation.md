# BCP chunk limits and streaming buffer validation

- Date: 2026-07-16
- Base commit: `1d434eb26d5f20a92ec145b0c608b44d00040505` (`v0.72.3`)
- PR branch: `codex/bcp-chunk-limits`
- Integration branch: `codex/bcp-chunk-limits-integration`
- Specification: `docs/superpowers/specs/2026-07-14-bcp-chunk-limits-and-stream-buffer-contract.md`
- Task contract: `test_artifacts/agent-policy/bcp-chunk-limits-task-contract.yml`

## Results

| Status | Check | Result |
| --- | --- | --- |
| PASS | Task contract validator | 0 errors, 0 warnings |
| PASS | Focused physical/streaming/FIFO/process/staging/type-fidelity tests | 111 passed |
| PASS | Full non-live regression | 4600 passed, 473 skipped in 383.92s |
| PASS | Change-aware Airflow, CLI, connector/route, schema/compatibility, and runtime/state subsets | All selected tests completed without failures |
| PASS | Ruff check | All checks passed |
| PASS | Ruff format check | 3109 files already formatted |
| PASS | Mypy | No issues in 506 source files |
| PASS | Import rules | No violations |
| PASS | Layer metrics | `intra_ratio=0.701`, `cross_ratio=0.299`; no issues |
| PASS | Module size | No hard-limit failures; 9 pre-existing warnings reported |
| PASS | Architecture fitness | No findings; average clustering 0.180 |
| PASS | Documentation links/contracts | 425 Markdown files and 1755 local links checked |
| PASS | Generated references | 2 of 2 in sync |
| PASS | Documentation language contracts | 4 passed |
| PASS | MkDocs strict build | Build completed successfully |
| PASS | Compatibility policy | 19 registry entries in sync |
| PASS | Core/native-accel/Airflow-pack builds | Six artifacts built in `/tmp/dpone-bcp-pr324-dist.2UQgQK` |
| PASS | Twine artifact validation | All six wheel/sdist artifacts passed |
| PASS | Live-test collection | 2 route cases collected: pipe streaming and physical chunks |
| SKIP / UNVERIFIED | Local MSSQL to ClickHouse execution | Docker daemon and approved live endpoints were unavailable; both cases skipped before source I/O |
| PASS | Independent fresh-context review | GO; no actionable findings against the approved contract |

## Safety evidence

Focused tests cover malformed and invalid limits before source I/O, exact and
crossing row boundaries, final unterminated rows, oversized complete and
pending rows, data-safe typed errors, no unconsumed pre-created chunks, BCP
termination, FIFO and partial-file cleanup, bounded process reap/kill/drainer
behavior, exception-safe raw and decoded ClickHouse staging cleanup,
schema/runtime parity, compatibility precedence, effective/deprecated decision
evidence, and default/custom FIFO frame bounds.

The final local review added a regression for the completed-iteration path:
when an inner byte-stream iterator raises from `close()`, artifact cleanup still
runs before the typed cleanup failure is propagated. Loader failures remain
primary and receive redacted cleanup context. Evidence tests prove that loader
exception bodies, source row values, untrusted failure codes, and another
artifact's owned files cannot cross the evidence or cleanup boundaries.

No live route is claimed as `PASS`. The disposable local-service command remains:

```bash
DPONE_RUN_INTEGRATION=1 uv run pytest \
  tests/integration/mssql/test_mssql_clickhouse_pipe_streaming_integration.py \
  -m "integration_mssql and integration_clickhouse" -q -rs
```

## Independent review

An independent read-only `dpone_architect` reviewer inspected the committed
candidate diff `origin/master...dc774e16` against the approved bug contract,
with emphasis on data-loss risk, row-boundary limits, FIFO/process cleanup,
ClickHouse staging lifecycle, schema compatibility, and evidence redaction.
The verdict was `GO: no actionable findings`. The reviewer confirmed that no
ADR is required for this scoped contract correction and retained live
MSSQL-to-ClickHouse execution as explicit residual `UNVERIFIED` risk.
