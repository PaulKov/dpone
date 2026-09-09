# Test artifacts for 2026-06-01

| Artifact | Type | Result | Notes |
| --- | --- | --- | --- |
| `2026-06-01T181000Z-static-quality-gate.json` | static quality | passed | ruff, format, mypy |
| `2026-06-01T181100Z-postgres-pgoutput-cdc-live.json` | live integration | passed | PostgreSQL pgoutput CDC |
| `2026-06-01T181200Z-mssql-cdc-live.json` | live integration | passed | MSSQL CDC + Change Tracking |
| `2026-06-01T181300Z-oss-coverage-build-gate.json` | OSS gate | passed | pytest coverage + build |
| `2026-06-01T182700Z-heavy-type-stress-50m-10gb-preflight.json` | heavy stress preflight | blocked | first matrix ordering; kept as diagnostic artifact |
| `2026-06-01T183000Z-heavy-type-stress-50m-10gb-preflight.json` | heavy stress preflight | blocked | corrected 100-column matrix; insufficient local disk |
| `2026-06-01T183100Z-heavy-type-stress-1000rows-smoke.json` | heavy stress smoke | failed | diagnostic: bcp datetimeoffset/varbinary format issue found |
| `2026-06-01T183200Z-heavy-type-stress-1000rows-smoke.json` | heavy stress smoke | passed | corrected native bcp formats, 100 columns verified |
| `2026-06-01T183300Z-test-artifacts-validation-gate.json` | artifact validation | passed | JSON validity + ruff/format after adding artifacts |

The requested 50M-row / 10GB run was not started because local preflight found insufficient disk headroom: 22.095 GiB available vs 35.0 GiB required by the conservative multiplier. The smaller 1000-row 100-column smoke passed on the same native path: PostgreSQL `COPY TO STDOUT` -> Microsoft `bcp in` -> SQL Server verification.
