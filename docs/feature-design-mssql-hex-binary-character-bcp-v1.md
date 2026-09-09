# Feature design: MSSQL hex-binary character BCP wire v1

- Status: APPROVED
- Owner: dpone maintainers
- Issue: Phase B1 of postgres route live wide certification
- Target release: next patch after stack merges
Last verified: 2026-07-23
Approval: maintainer APPROVED 2026-07-23 — approach A (hex on character
  `mssql-delimited` + `CONVERT(varbinary, …, 2)` on staging→target)

## Executive summary

Character `mssql-delimited` BCP fail-closes when staging DDL contains
`varbinary`/`binary`, so postgres `bytea` and mysql `BLOB` were documented N/A.
This change makes binary columns land as **hex text** on the character wire,
stage as `nvarchar(max)`, and decode with set-based
`CONVERT(<varbinary>, <col>, 2)` into the target `varbinary` contract — without
`allow_unsafe_raw_mssql_bulk_files` and without a proprietary `bcp -n` producer
from Postgres.

## Personas and customer journey

| Persona | Goal | Pain | Success |
|---|---|---|---|
| Data engineer | Load `bytea`/`BLOB` into MSSQL on Docker IT / prod BCP path | N/A or unsafe override | Wide live + guide claim binary via hex character BCP |
| Maintainer | One binary wire for mysql→mssql and postgres→mssql | Split stories | Shared staging decode + extract projection |

## Scope

### In scope

- Staging rewrite: binary/varbinary/image → `nvarchar(max)` for character BCP
- Finalize/select decode: `CONVERT(varbinary…, col, 2)` for hex columns
- Postgres COPY wrap: `encode(bytea,'hex')` when sink schema is binary family
- MySQL already emits hex for `bytes` on mssql-delimited (keep)
- Python `insert_rows` path: bytes → hex
- Wide live IT includes `bytea`/`BLOB`; hermetic contracts
- Docs: remove character-BCP N/A for hex-safe binary; note true `bcp -n` producer as follow-up

### Non-goals

- SQL Server native (`bcp -n`) file producer from Postgres/MySQL
- `rowversion` / geometry / geography on character BCP (remain fail-closed)
- ClickHouse scd2/diff (Phase B2)

## Public contract

No new CLI commands. Additive artifact metadata:
`StagingTableArtifact.hex_binary_columns` + `target_column_types` for decode.
Target DDL remains pair-profile `varbinary(max)`.

## Algorithm

```text
1. Extract schema maps source binary → varbinary(max) (unchanged pair profile)
2. Source export projects hex text for those columns (PG encode / MySQL bytes.hex)
3. Staging CREATE uses nvarchar(max) for hex-binary columns; record hex_binary_columns
4. Character bcp loads hex text (BulkTextCodec may wrap nvarchar)
5. Staging→target SELECT: BulkTextCodec decode then CONVERT(varbinary, expr, 2)
6. Target/shadow DDL keeps varbinary from payload.schema
```

## Test plan

| Layer | Evidence |
|---|---|
| Hermetic | staging rewrite + select expression CONVERT; bulk safety still rejects rowversion |
| Live | postgres→mssql + mysql→mssql wide FR (min) with binary column typed assert |

## Rollout

Stacked PR on postgres wide-cert tip; update matrix/guides/CHANGELOG after PASS.
