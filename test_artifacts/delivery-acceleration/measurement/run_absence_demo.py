"""Execute the documented offline producer/consumer journey without live flags.

Only the canonical tools produce report JSON. This runner records their argv,
exit status and output; it never substitutes synthetic success evidence.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict
from pathlib import Path

from dpone.contracts.mssql_native_chunks import NativeChunkLimits


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    output = Path(__file__).resolve().parent / "absence-demo"
    output.mkdir(exist_ok=False)
    limits = output / "limits.json"
    limits.write_text(json.dumps(asdict(NativeChunkLimits(104857600, 104857600)), indent=2) + "\n")
    environment = dict(os.environ)
    for name in ("DPONE_RUN_INTEGRATION", "DPONE_RUN_INTEGRATION_LIVE", "DPONE_DDA_DISPOSABLE_APPROVED"):
        environment.pop(name, None)
    commands = []
    for side in ("baseline", "candidate"):
        (output / side).mkdir()
        commands.append(
            [
                "uv",
                "run",
                "python",
                "tools/native_delivery_live_benchmark.py",
                "run",
                "--profile",
                "unicode",
                "--rows",
                "10000",
                "--seed",
                "7",
                "--trials",
                "3",
                "--limits",
                str(limits),
                "--output",
                str(output / side / "run.json"),
            ]
        )
    commands.extend(
        [
            [
                "uv",
                "run",
                "python",
                "tools/native_delivery_live_benchmark.py",
                "inspect",
                str(output / "candidate/run.json"),
            ],
            [
                "uv",
                "run",
                "python",
                "tools/native_delivery_benchmark.py",
                "compare",
                "--baseline",
                str(output / "baseline/run.json"),
                "--candidate",
                str(output / "candidate/run.json"),
                "--output",
                str(output / "comparison.json"),
            ],
        ]
    )
    results = []
    for command in commands:
        result = subprocess.run(command, cwd=root, env=environment, capture_output=True, text=True, check=False)
        results.append(
            {"command": command, "exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
        )
        if result.returncode:
            break
    (output / "commands.json").write_text(json.dumps(results, indent=2) + "\n")
    if len(results) != len(commands) or any(item["exit_code"] for item in results):
        raise SystemExit("Offline journey failed; inspect commands.json")
    comparison = json.loads((output / "comparison.json").read_text())
    assert comparison["status"] == "UNVERIFIED"
    assert comparison["workloads"][0]["ratio"] is None
    print("PASS: offline artifact interoperability; live SKIP, performance UNVERIFIED")


if __name__ == "__main__":
    main()
