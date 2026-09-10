"""Reusable hermetic protocol fixture; never live execution or certification authority."""

from pathlib import Path

from .artifacts import ArtifactStore
from .execution import ExecutionAdapter, Snapshot
from .profiles import Dataset
from .runner import configuration, route_record, run_benchmark


class HermeticRouteSession:
    """Local protocol double; intentionally incapable of conferring live authority."""

    def __init__(self, dataset, case, clock):
        self.invocation_id = "58ca9aad-f6a9-47cd-a7a2-1fe9eebc4e00"
        self.dataset, self.case, self.clock = dataset, case, clock
        self.rows = [{"old": 1}]
        self.outside = [{"outside": "unchanged"}]
        self.queries, self.publications, self.stage_reads = 0, 0, 0
        self.known, self.cleaned, self.closed = True, False, False
        self.fault = None
        self.events, self.probes, self.complete = (), 0, False

    def snapshot(self):
        return Snapshot(
            self.rows,
            self.outside,
            "a" * 64,
            "a" * 64,
            "b" * 64,
            "b" * 64 if self.publications else None,
            self.queries,
            self.publications,
            self.stage_reads,
            self.known,
            128,
            fault_events=self.events,
            receipt_probes=self.probes,
            pipeline_complete=self.complete,
        )

    def arm_fault(self, fault):
        self.fault = fault

    def run(self):
        self.clock.source_acquired()
        self.queries += 1
        if self.fault:
            self.events += (self.fault,)
        if self.fault in {"before_commit", "after_eof"}:
            raise RuntimeError("secret driver text must never be reported")
        self._publish()
        if self.fault == "unknown_commit":
            self.known = False
            self.complete = False
            raise RuntimeError("secret connection URL")
        if self.fault == "lost_ack":
            self.probes += 1
        self.clock.committed_visible()

    def _publish(self):
        self.rows = list(self.dataset.generate())
        self.publications += 1
        self.complete = True

    def recover(self, *, source_allowed):
        assert source_allowed is False
        if not self.known:
            raise RuntimeError("unknown commit blocks replay")
        if not self.publications:
            self._publish()

    def cleanup(self):
        assert self.known
        self.cleaned = True

    def close(self):
        self.closed = True


class HermeticRouteFactory:
    execution = "hermetic"
    subject_checkout = Path(__file__).resolve().parents[2]

    def __init__(self):
        self.sessions = []
        self.layout = "c" * 64

    def describe(self):
        return {
            "versions": {key: "hermetic-1" for key in ("python", "dpone", "clickhouse", "mssql", "bcp")},
            "target_layout_sha256": self.layout,
            "resource_profile": {"cpu_count": 1},
        }

    def open(self, dataset, *, case, clock):
        session = HermeticRouteSession(dataset, case, clock)
        self.sessions.append(session)
        return session


LIMITS = {
    "max_total_encoded_bytes": 104857600,
    "stage_allocated_bytes_stop_threshold": 104857600,
    "max_rows": 16,
    "max_bytes": 1048576,
    "max_row_bytes": 65536,
    "max_pending": 2,
    "max_staging_tables": 128,
    "parallelism": 1,
}


def produce_fixture(tmp_path, factory=None):
    return run_benchmark(
        adapter=ExecutionAdapter(factory or HermeticRouteFactory(), "candidate"),
        dataset=Dataset("unicode", 16),
        config=configuration(LIMITS),
        route=route_record("partition_replace", "bounded_native"),
        store=ArtifactStore(tmp_path / "run.json"),
    )
