"""Best-effort sampled process-tree RSS, explicitly excluding external SQL servers."""

from __future__ import annotations

import os
import subprocess
import threading

from .execution import measured, unavailable


class ProcessTreeRss:
    """Sample coordinator and descendants using numeric ps output on Linux/macOS.

    Short-lived processes between samples can be missed. This is an observed
    sampled maximum, not a continuous peak or the sum of per-process peaks.
    """

    def __init__(self, interval: float = 0.05):
        self.interval = interval
        self.root = os.getpid()
        self.peak = 0
        self.samples = 0
        self.processes: set[int] = set()
        self.failed = False
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=3)
        if self.thread.is_alive():
            self.failed = True

    def _sample(self) -> None:
        while not self.stop.is_set():
            try:
                with subprocess.Popen(
                    ["ps", "-axo", "pid=,ppid=,rss="],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                ) as process:
                    try:
                        output, _ = process.communicate(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate()
                        raise
                    if process.returncode:
                        raise ValueError("ps_failed")
                    table = {
                        pid: (parent, rss)
                        for pid, parent, rss in (map(int, line.split()) for line in output.splitlines())
                        if pid != process.pid
                    }
                selected = {self.root}
                while additions := {pid for pid, (parent, _) in table.items() if parent in selected} - selected:
                    selected |= additions
                if self.root not in table:
                    raise ValueError("root_rss_unavailable")
                self.peak = max(self.peak, sum(table[pid][1] * 1024 for pid in selected))
                self.processes |= selected
                self.samples += 1
            except (OSError, ValueError, subprocess.SubprocessError):
                self.failed = True
                return
            self.stop.wait(self.interval)

    def metric(self):
        if self.failed or not self.samples:
            return unavailable("bytes", "process_tree_sampling_unavailable")
        provenance = (
            f"ps-rss-kib:process-tree:root={self.root}:interval_ms={self.interval * 1000:g}"
            f":samples={self.samples}:pids={','.join(map(str, sorted(self.processes)))}"
        )
        return measured(self.peak, "bytes", provenance)
