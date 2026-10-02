"""Scripted transport beneath real recovery/catalog/DDL adapters, not live proof."""

import json
from types import SimpleNamespace
from uuid import uuid4

from dpone.app.publication_operator_context import resolve_authority, verified_context
from dpone.contracts.publication_authority_binding import publication_binding_digest
from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog
from tests.test_native_prepared_recovery import Authority, Safety
from tests.test_publication_schema_application import operator_context
from tests.test_runtime_connection_context_loader import _replace_plan_descriptor


def recovery_context(tmp_path, monkeypatch):
    environ, root = operator_context(tmp_path)
    path = root / "connection-registry.json"
    registry = json.loads(path.read_bytes())
    registry["connections"]["sink-registry"] = {
        "type": "clickhouse",
        "connection": {"host": "synthetic-clickhouse", "database": "analytics", "driver": "native", "port": 9000},
        "credentials": {
            "resolver": "airflow_env",
            "connection_id": "synthetic-target",
            "payload_format": "airflow_connection_uri",
        },
    }
    content = json.dumps(registry, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(content)
    monkeypatch.setenv(
        "AIRFLOW_CONN_SYNTHETIC_TARGET", "clickhouse://user:synthetic-private@synthetic-clickhouse:9000/analytics"
    )
    return _replace_plan_descriptor(environ, name="connection_registry", content=content), root


class ClickHouseServer:
    hosts = ("node-1", "node-2")
    columns = (("id", "UInt64", "", "", 1),)

    def __init__(self):
        self.server_uuid, self.database_uuid = str(uuid4()), str(uuid4())
        self.tables = {
            name: (str(uuid4()), f"ReplicatedMergeTree('/tables/{name}', '{{replica}}')", f"/tables/{name}", 2)
            for name in ("target", "candidate")
        }
        self.effects, self.queue, self.handles = [], [], []
        self.lose_reply = False
        self.fail_close = False
        self.driver = "native"

    def connect(self, resolved):
        assert resolved.descriptor.connection_type == "clickhouse", "no business source connection allowed"
        handle = ClickHouseHandle(self)
        self.handles.append(handle)
        return handle


class ClickHouseHandle:
    host, port, secure, database = "synthetic-clickhouse", 9000, False, "analytics"

    def __init__(self, server):
        self.server = server
        self.driver = server.driver
        self.connection = self
        self.closed = 0

    def get_records(self, query, params=None):
        assert not self.closed
        state = self.server
        if "serverUUID()" in query:
            assert "'analytics'" in query
            return [(state.server_uuid, "analytics", state.database_uuid, "Atomic")]
        assert params["cluster"] == "cluster"
        if "database" in params:
            assert params["database"] == "analytics"
        if "system.clusters" in query:
            return [(host, f"127.0.0.{index}", 9000, 1, index, 1) for index, host in enumerate(state.hosts, 1)]
        if "system.databases" in query:
            return [(host, "Atomic") for host in state.hosts]
        if "system.distributed_ddl_queue" in query:
            return [
                row
                for row in state.queue
                if row[2 if "token" in params else 0] == params.get("token", params.get("entry"))
            ]
        if "count()" in query:
            names = [name for name in state.tables if f"`analytics`.`{name}`" in query]
            assert len(names) == 1
            return [(host, state.tables[names[0]][3]) for host in state.hosts]
        assert set(params["names"]) == {"target", "candidate"}
        rows = []
        for host in state.hosts:
            for name, (identity, engine, keeper, count) in state.tables.items():
                if "system.tables" in query:
                    rows.append((host, name, identity, engine, count))
                elif "system.columns" in query:
                    rows.append((host, name, state.columns))
                elif "system.replicas" in query:
                    rows.append((host, name, "default", keeper, 0, 0, 0, 2, 2, "", ""))
                else:
                    raise AssertionError("unexpected read")
        return rows

    def execute(self, query, *, settings, query_id):
        assert not self.closed
        assert settings["skip_unavailable_shards"] == 0
        assert settings["distributed_ddl_output_mode"] == "throw"
        assert query_id
        state = self.server
        if query == "EXCHANGE TABLES `analytics`.`target` AND `analytics`.`candidate` ON CLUSTER `cluster`":
            state.tables["target"], state.tables["candidate"] = state.tables["candidate"], state.tables["target"]
        elif query == "DROP TABLE IF EXISTS `analytics`.`candidate` ON CLUSTER `cluster`":
            del state.tables["candidate"]
        else:
            raise AssertionError("unexpected mutation")
        state.effects.append(query)
        for host in state.hosts:
            state.queue.append((str(len(state.effects)), query, settings["log_comment"], host, "Finished", 0, ""))
        if state.lose_reply:
            raise TimeoutError("synthetic-private lost reply")

    def close(self):
        self.closed += 1
        if self.server.fail_close:
            raise RuntimeError("synthetic-private close failed")


def operator_rig(tmp_path, monkeypatch):
    environ, root = recovery_context(tmp_path, monkeypatch)
    context = verified_context(environ, "prod")
    sql, binding, pin = resolve_authority(context, "source-main")
    server = ClickHouseServer()
    catalog = ClickHouseClusterPublicationCatalog(ClickHouseHandle(server))
    hosts = catalog.inventory("cluster").hosts
    facts = catalog.generations("cluster", "analytics", "target", "candidate", hosts)
    authority = Authority(SimpleNamespace(old=facts[0].target, new=facts[0].candidate, inventory=catalog.inventory))
    authority.binding = publication_binding_digest(binding, endpoint_identity=pin)
    calls = []

    def build(**kwargs):
        assert kwargs["connection"] == sql
        assert kwargs["binding"] == binding
        assert kwargs["environment"] == "prod"
        calls.append(kwargs)
        return authority

    monkeypatch.setattr("dpone.app.prepared_recovery_application.build_publication_authority", build)
    return SimpleNamespace(
        environ=environ, root=root, server=server, authority=authority, safety=Safety(catalog), builds=calls
    )
