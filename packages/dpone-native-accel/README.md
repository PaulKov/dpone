# dpone-native-accel

`dpone-native-accel` is the optional provider package for dpone native-transfer
acceleration. It contains the certified MSSQL BCP native -> ClickHouse Native
block encoder and the direct ClickHouse Native TCP ingest backend.

The direct ingest backend sends pre-encoded ClickHouse Native blocks through the
Native TCP protocol without spawning `clickhouse-client`. `native_tcp.backend:
auto` selects this backend when the provider is installed and certified;
`backend: client` keeps the subprocess fallback for compatibility benchmarks.

Optional provider package for dpone native-transfer acceleration.

The package exposes the stable provider boundary used by `dpone[accel]`.
Backends declare certified capabilities before dpone can select them for
`native_transfer.wire.acceleration.mode: auto|required`.

The v0.74 provider includes a fused MSSQL BCP native -> ClickHouse Native block
encoder and a direct ClickHouse Native TCP insert backend. Direct insert uses a
native-protocol `INSERT ... VALUES` query plus protocol `Data` packets; it does
not shell out to `clickhouse-client` and does not materialize Python row
objects.

The legacy `transcode()` API continues to yield raw Native blocks. The dpone
runtime uses the additive `transcode_batches()` boundary, whose closed mapping
binds each block to the row count observed by the encoder before serialization;
raw bytes or malformed batch mappings fail closed and cannot produce successful
acceleration evidence.

Certification covers signed/unsigned integers, bool, Float32/Float64 finite
extremes, Decimal128/Decimal256 max/min scaled values, nullable decimals,
Unicode text, tabs/newlines, binary payloads, UUID, Date/Date32,
DateTime/DateTime64 precision, SQL Server `time` and `datetimeoffset`,
FixedString padding and oversize rejection, and NULL versus empty values.
`LowCardinality`, composite, and enum Native targets fail closed because they
require dictionary or nested serialization rather than scalar bytes.
Unsupported source layouts fall back to the reference path unless acceleration
is explicitly required.
