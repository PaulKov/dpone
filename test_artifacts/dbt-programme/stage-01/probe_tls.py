"""Observe existing ClickHouse option projection; reject network and subprocess I/O.

Run with ``uv run --frozen python -B <this file>``. Assertions describe baseline
behavior, including gaps; they are not a TLS handshake or certification test.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.sinks.clickhouse_bulk_mixin import ClickHouseBulkMixin


class CaptureRunner:
    def __init__(self, credentials, options):
        self.credentials = credentials
        self.options = options


class ProbeSink(ClickHouseBulkMixin):
    _client_runner_cls = CaptureRunner
    _http_runner_cls = CaptureRunner
    _direct_native_runner_cls = CaptureRunner


def forbidden(*args, **kwargs):
    raise AssertionError("unexpected network or subprocess")


def observe_bulk(mode, options, expected):
    sink = ProbeSink()
    sink.connector = SimpleNamespace(
        host="synthetic.invalid",
        port=8443 if mode == "http" else 9440,
        database="synthetic",
        user="synthetic",
        password="synthetic-unused-value",
        secure=True,
        ca_cert="/synthetic/ca.pem",
    )
    config = SimpleNamespace(options={"clickhouse_bulk": {"mode": mode, mode: options}})
    method = sink._build_http_runner if mode == "http" else sink._build_client_runner
    runner = method(config)
    observed = runner.credentials.secure, runner.credentials.port
    assert observed == expected
    if mode == "native_tcp":
        direct = sink._build_direct_native_runner(config)
        assert (direct.credentials.secure, direct.credentials.port) == expected
    return {
        "mode": mode,
        "options": options,
        "secure": observed[0],
        "port": observed[1],
        "ca_field": hasattr(runner.credentials, "ca_cert"),
    }


def main():
    source_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    native_calls, http_calls = [], []
    native_module, http_module = ModuleType("clickhouse_driver"), ModuleType("clickhouse_connect")

    def native_client(**kwargs):
        native_calls.append(kwargs)
        return object()

    def http_client(**kwargs):
        http_calls.append(kwargs)
        return object()

    native_module.Client = native_client
    http_module.get_client = http_client
    results = []
    with (
        patch.object(socket, "socket", forbidden),
        patch.object(socket, "create_connection", forbidden),
        patch.object(subprocess, "Popen", forbidden),
        patch.dict(sys.modules, {"clickhouse_driver": native_module, "clickhouse_connect": http_module}),
    ):
        for mode, options, expected in (
            ("http", {}, (False, 8123)),
            ("http", {"secure": True}, (True, 8123)),
            ("http", {"secure": True, "port": 8443}, (True, 8443)),
            ("http", {"secure": False, "port": 8124}, (False, 8124)),
            ("native_tcp", {}, (False, 9000)),
            ("native_tcp", {"secure": True}, (True, 9000)),
            ("native_tcp", {"secure": True, "port": 9440}, (True, 9440)),
            ("native_tcp", {"secure": False, "port": 9001}, (False, 9001)),
            ("client", {}, (True, 9440)),
            ("client", {"secure": False, "port": 9002}, (False, 9002)),
        ):
            results.append(observe_bulk(mode, options, expected))
        for driver in ("native", "http"):
            connector = ResolvedConnectorFactory.clickhouse(
                CredentialsConfig(
                    host="synthetic.invalid",
                    database="synthetic",
                    username="synthetic",
                    password="synthetic-unused-value",
                    secure=True,
                    driver=driver,
                    additional_params={"ca_cert": "/synthetic/ca.pem"},
                )
            )
            assert connector.ca_cert == "/synthetic/ca.pem"
            _ = connector.connection
            captured = native_calls[-1] if driver == "native" else http_calls[-1]
            ca_forwarded = captured.get("ca_cert") == "/synthetic/ca.pem"
            assert ca_forwarded is (driver == "http")
            results.append(
                {
                    "driver": driver,
                    "secure": captured["secure"],
                    "port": captured["port"],
                    "ca_retained_on_connector": True,
                    "ca_forwarded_to_client": ca_forwarded,
                }
            )
    print(
        json.dumps(
            {
                "source_commit": source_commit,
                "network_subprocess_blocked": True,
                "status": "PASS",
                "meaning": "baseline behavior reproduced; live TLS SKIP",
                "observations": results,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
