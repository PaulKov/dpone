"""Factory entry point loaded only after the harness's explicit live approval."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from uuid import UUID, uuid4

from tools.native_delivery_live_support.execution import DeliveryClock, loaded_subject
from tools.native_delivery_live_support.runner import configuration as validate_configuration

from .descriptor import observe
from .environment import Environment
from .inventory import Inventory, InventoryStore
from .provisioning import provision
from .session import Session


class LocalRouteFactory:
    execution = "live"

    def __init__(self, *, configuration, route, environment=None):
        if route.get("strategy") not in {"full_refresh", "partition_replace"} or route.get("mode") != "bounded_native":
            raise ValueError("local_fixture.unsupported_route")
        if validate_configuration(configuration["limits"]) != configuration:
            raise ValueError("local_fixture.configuration_digest")
        self.configuration, self.route = configuration, route
        self.environment = environment or Environment(
            root=Path(os.environ.get("DPONE_DDA_SPOOL_ROOT", "/dda/artifacts/local-fixtures"))
        )
        self.inventory_store = InventoryStore(self.environment.root)
        self.subject_checkout = loaded_subject()
        self._descriptor = None

    def prepare_benchmark(self):
        """Observe versions/layout once; never call this on maintenance paths."""
        if self._descriptor is None:
            self._descriptor, self.layouts = observe(self.environment, self.subject_checkout)
        return self

    def describe(self):
        """Return detached immutable observations without opening connections."""
        if self._descriptor is None:
            raise RuntimeError("local_fixture.benchmark_not_prepared")
        return deepcopy(self._descriptor)

    def open(self, dataset, *, case, clock):
        invocation = uuid4().hex
        value = Inventory(
            invocation,
            dataset.profile,
            dataset.rows,
            dataset.seed,
            self.route["strategy"],
            self.environment.database,
            self.environment.database,
            self.environment.source_database,
            "dda_" + invocation,
            "business",
            self.configuration,
            {},
            {},
        )
        self.inventory_store.create(value)
        results = provision(self.environment, self.inventory_store, value)
        if hasattr(self, "layouts") and results["target_layout"] != self.layouts[dataset.profile]:
            raise ValueError("local_fixture.target_layout_changed")
        return Session(self.environment, self.inventory_store, value, clock)

    def attach(self, invocation_id):
        parsed = UUID(invocation_id)
        if str(parsed) != invocation_id:
            raise ValueError("local_fixture.canonical_invocation_required")
        value = self.inventory_store.load(parsed.hex)
        if value.configuration != self.configuration or value.strategy != self.route["strategy"]:
            raise ValueError("local_fixture.recovery_configuration_changed")
        return Session(self.environment, self.inventory_store, value, DeliveryClock())


def create_factory(*, configuration, route):
    return LocalRouteFactory(configuration=configuration, route=route)


def create_benchmark_factory(*, configuration, route):
    return create_factory(configuration=configuration, route=route).prepare_benchmark()
