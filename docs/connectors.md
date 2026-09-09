# Connector Overview

This page is the balanced connector entrypoint for dpone. The README intentionally links here instead of highlighting only one or two systems, so users can find every supported family from one place.

## Database and warehouse connectors

| System | Role | Install extra | Detailed docs | Source -> sink examples |
| --- | --- | --- | --- | --- |
| PostgreSQL | source, sink, state, CDC/XMin source | `dpone[postgres]` | [Postgres XMin deep dive](postgres-xmin.md), [State](state.md), [CDC](cdc.md) | [Source -> sink matrix](source-sink-matrix.md) |
| MySQL | batch source | `dpone[mysql]` | [Source -> sink matrix](source-sink-matrix.md) | [MySQL guides](source-sink-matrix.md#matrix) |
| MSSQL / SQL Server | source, sink, state, CDC/CT contracts | `dpone[mssql]` | [MSSQL](mssql.md) | [MSSQL guides](source-sink-matrix.md#matrix) |
| ClickHouse | source, sink | `dpone[clickhouse]` | [ClickHouse](clickhouse.md) | [ClickHouse guides](source-sink-matrix.md#matrix) |
| BigQuery / GCS | sink, state, GCS bulk path | `dpone[gcp]` | [State](state.md), [Release/ops docs](operations.md) | [BigQuery guides](source-sink-matrix.md#matrix) |

## API and event connectors

| System | Role | Install extra | Detailed docs |
| --- | --- | --- | --- |
| Generic REST API | source | core package plus target extras | [REST API](rest-api.md) |
| Kafka | bounded batch source, event-log sink | `dpone[kafka]` | [Kafka](kafka.md) |

## Managed provider connectors

| Provider | Role | Detailed docs |
| --- | --- | --- |
| AppsFlyer Pull API | source | [AppsFlyer](appsflyer.md) |
| CBR XML API | source | [CBR](cbr.md) |
| FastTrack Pull API | source | [FastTrack](fasttrack.md) |
| Google Ads API | source | [Google Ads](google-ads.md) |
| Google Sheets API | source | [Google Sheets](google-sheets.md) |
| Mindbox Pull API | source | [Mindbox](mindbox.md) |
| OpenExchangeRates API | source | [OpenExchangeRates](openexchangerates.md) |
| SimilarWeb API | source | [SimilarWeb](similarweb.md) |
| Yandex Webmaster API | source | [Yandex Webmaster](yandex-webmaster.md) |

## What to read next

- Use [Studio/capability discovery](studio.md) to compare support,
  certification evidence, and beginner recipe availability.
- Use [Connector certification](connector-certification.md) for the independent
  status axes and report gate.
- Use [Source -> sink matrix](source-sink-matrix.md) for concrete source-target combinations.
- Use [Load strategies](load-strategies.md) for `full_refresh`, append, upsert, replace, XMin, CDC, and Kafka offset behavior.
- Use [Type mapping matrix](type-mapping-matrix.md) when moving vendor-specific data types.
- Use [Schema evolution](schema-evolution.md) when source schemas drift.
- Use [Connector SDK](connector-sdk.md) to scaffold community connectors with package metadata, examples, tests, and certification evidence.
