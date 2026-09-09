# Kafka batch source/sink

`dpone[kafka]` adds Kafka as a first-class bounded batch backend:

- `source.type: kafka` reads a finite batch from a topic.
- `sink.type: kafka` produces rows from any dpone source into a topic.
- Default source boundary is fixed offsets, not infinite streaming.
- Default offset state is dpone state storage; Kafka consumer commits are optional.
- Default sink delivery is at-least-once; idempotent producer is opt-in.

## Install

```bash
pip install "dpone[kafka]"
```

`dpone[full]` also includes Kafka support.

## Credentials

Kafka credentials can come from `env`, `airflow`, `vault`, or `params`.

Required field:

| Field | Description |
| --- | --- |
| `bootstrap_servers` | Kafka bootstrap servers, for example `localhost:9092` |

Optional fields:

| Field | Description |
| --- | --- |
| `security_protocol` | Kafka security protocol |
| `sasl_mechanism` | SASL mechanism |
| `sasl_username` | SASL username |
| `sasl_password` | SASL password, redacted in logs |
| `ssl_ca_location` | CA bundle path |
| `client_id` | Kafka client id; default `dpone` |
| `schema_registry_url` | Confluent-compatible Schema Registry URL |
| `schema_registry_username` | Schema Registry username |
| `schema_registry_password` | Schema Registry password, redacted in logs |

Environment example for `connection_id: kafka_cluster`:

```bash
export KAFKA_CLUSTER_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CLUSTER_SCHEMA_REGISTRY_URL=http://localhost:8081
```

## Kafka source

Kafka source reads bounded batches. It fixes the batch boundary before reading so retries are deterministic.

```yaml
source:
  type: kafka
  connection_id: kafka_cluster
  connection_type: vault
  topic: orders_events
  options:
    group_id: dpone.orders.batch
    read_mode: offsets
    offset_storage: dpone
    start_from: stored
    message_format: json
    envelope: auto
    batch_size: 50000
    poll_timeout_ms: 1000
    max_empty_polls: 3
```

### Read modes

| Mode | Behavior | Use when |
| --- | --- | --- |
| `offsets` | Reads `[stored_or_start, current_high_watermark)` | Default ETL runs and exact resume |
| `time_window` | Looks up offsets by timestamp window | Hourly/daily topic slices |
| `max_records` | Reads up to `max_records` | Controlled smoke/backfill batches |

### Offset storage

| Mode | Behavior |
| --- | --- |
| `dpone` | Stores offsets in dpone state after sink success. Default. |
| `kafka` | Commits Kafka consumer offsets after sink success. |

Kafka offset state table default: `etl_state.etl_kafka_offsets`.

## Kafka sink

Kafka sink produces rows from any source to Kafka.

```yaml
sink:
  type: kafka
  connection_id: kafka_cluster
  connection_type: vault
  topic: dwh.orders
  strategy: {mode: incremental_append, unique_key: order_id}
  options:
    message_format: avro
    envelope: flat
    key:
      mode: unique_key
    delivery:
      mode: at_least_once
      compression_type: zstd
      linger_ms: 20
      batch_num_messages: 10000
    schema_registry:
      enabled: true
      subject_name_strategy: topic
      auto_register_schemas: true
```

### Delivery modes

| Mode | Behavior |
| --- | --- |
| `at_least_once` | Produces batches, waits for delivery callbacks and flushes. Default. |
| `idempotent` | Enables Kafka producer idempotence and `acks=all`. |

Kafka transactions are intentionally out of scope for this batch ETL increment.

## Message formats

| Format | Schema Registry | Notes |
| --- | --- | --- |
| `json` | Optional | Default. Plain JSON object rows. |
| `json_schema` | Required | Confluent-compatible JSON Schema subject. |
| `avro` | Required | Confluent-compatible Avro subject. |
| `protobuf` | Required | Generated message class or proto config should be supplied for strict production use. |

## Key policy

| Mode | Behavior |
| --- | --- |
| `unique_key` | Builds key from `sink.strategy.unique_key`; fallback key is null. Default. |
| `hash_row` | Builds SHA-256 key from the whole row. Useful for append topics without natural keys. |
| `null` | Produces messages without keys. |

## Envelope and deletes

Default `envelope: flat` writes the row as Kafka value.

`envelope: dpone` writes:

```json
{
  "op": "upsert",
  "data": {"id": 1, "name": "Ada"},
  "metadata": {"strategy": "incremental_append"},
  "source": {"schema": "public", "table": "orders"},
  "schema_version": 1
}
```

For compacted topics or CDC-style streams:

```yaml
sink:
  options:
    envelope: dpone
    deletes:
      enabled: true
      tombstone: true
```

Tombstones require a non-null key. If a row carries `__dpone__op: delete`, dpone can emit a Kafka tombstone when `deletes.tombstone: true`.

## Schema evolution

With Schema Registry enabled, Kafka sink can read the latest value schema and register safe evolved schemas when:

```yaml
sink:
  options:
    schema_registry:
      enabled: true
      auto_register_schemas: true
```

Without Schema Registry, Kafka schema evolution is a no-op on the target side. Source schemas are still inferred from sampled decoded rows and passed to downstream sinks.

## Runbook

| Symptom | Action |
| --- | --- |
| Source replays from beginning | Check `offset_storage`, `group_id`, and state table availability. |
| Sink duplicates messages | Use `delivery.mode: idempotent` and provide a stable `unique_key`; downstream should deduplicate by key/event id. |
| Tombstone fails | Ensure key mode produces non-null keys. |
| Avro/Protobuf fails at startup | Ensure Schema Registry credentials are configured and subject exists or `auto_register_schemas: true`. |
| Large file source is slow | Use source partitioned exports and Kafka producer batching/compression settings. |
