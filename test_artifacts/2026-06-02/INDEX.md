# Test artifacts for 2026-06-02

| Artifact | Type | Result | Notes |
| --- | --- | --- | --- |
| `2026-06-02T000100Z-type-matrix-pg-mssql-ch-certification.json` | type certification | failed | harness bug: missing `TypeCase.source_sql`; kept as diagnostic evidence |
| `2026-06-02T000200Z-type-matrix-pg-mssql-ch-certification.json` | type certification | failed | SQL generation bug around XML `AS`; kept as diagnostic evidence |
| `2026-06-02T000300Z-type-matrix-pg-mssql-ch-certification.json` | type certification | failed | MSSQL rowversion raw-byte TSV issue; kept as diagnostic evidence |
| `2026-06-02T000400Z-type-matrix-pg-mssql-ch-certification.json` | type certification | failed | data loaded but verification ordering bug; kept as diagnostic evidence |
| `2026-06-02T000500Z-type-matrix-pg-mssql-ch-certification.json` | type certification | passed | PostgreSQL -> MSSQL 53 types and MSSQL -> ClickHouse 35 types verified by canonical hash |

## Passed certification scope

`2026-06-02T000500Z-type-matrix-pg-mssql-ch-certification.json` verifies:

- PostgreSQL -> MSSQL through PostgreSQL `COPY TO STDOUT` and SQL Server `bcp in`.
- MSSQL -> ClickHouse through SQL Server `bcp queryout` and ClickHouse HTTP `INSERT ... FORMAT TabSeparated`.
- Lossless canonical serialization for engine-specific/exotic types.
- Source and target row hashes matched in both directions.

## PostgreSQL type families covered

- Boolean, integer, floating point, numeric, money.
- Text, varchar, char, bytea.
- Date, time, timetz, timestamp, timestamptz, interval.
- UUID, JSON, JSONB, XML.
- Arrays, ranges, multiranges.
- Geometric types: point, line, lseg, box, path, polygon, circle.
- Network types: inet, cidr, macaddr.
- Bit strings, full-text search, pg_lsn, oid, regclass, regtype.
- Custom enum/domain/composite and hstore extension.

## MSSQL type families covered

- bit, tinyint, smallint, int, bigint.
- decimal, numeric, money, smallmoney, float, real.
- date, time, datetime, smalldatetime, datetime2, datetimeoffset.
- char, varchar, text, nchar, nvarchar, ntext.
- binary, varbinary, varbinary(max), image.
- uniqueidentifier, xml, sql_variant, hierarchyid, geometry, geography, rowversion.

## Notes

The certification defines correctness as canonical serialization for cross-engine exotic types. That is intentional: PostgreSQL `int4range` and SQL Server `hierarchyid` do not have identical ClickHouse/MSSQL physical types, so the reliable production contract is stable canonical value preservation.
