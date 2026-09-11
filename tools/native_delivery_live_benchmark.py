"""Opt-in real-row harness. No service discovery occurs on import/help/absence."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.native_delivery_live_support.artifacts import ArtifactStore
from tools.native_delivery_live_support.execution import ExecutionAdapter, git_identity, load_factory, loaded_subject
from tools.native_delivery_live_support.maintenance import invocation_id, maintain
from tools.native_delivery_live_support.profiles import PROFILES, Dataset
from tools.native_delivery_live_support.runner import absent_run, configuration, route_record, run_benchmark
from tools.native_delivery_live_support.validation import validate_run

APPROVAL_FLAGS = ("DPONE_RUN_INTEGRATION", "DPONE_RUN_INTEGRATION_LIVE", "DPONE_DDA_DISPOSABLE_APPROVED")


def approved(environment=None) -> bool:
    """Require all three explicit flags; host names or credentials do not authorize I/O."""
    source = os.environ if environment is None else environment
    return all(source.get(name) == "1" for name in APPROVAL_FLAGS)


class SafeParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(2, "native_delivery.invalid_arguments\n")


def parser() -> argparse.ArgumentParser:
    result = SafeParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True, parser_class=SafeParser)
    run = commands.add_parser(
        "run", help="Write one run envelope; no approval writes SKIP without loading the factory."
    )
    run.add_argument("--profile", choices=PROFILES, default="narrow")
    run.add_argument("--rows", type=int, default=10000)
    run.add_argument("--seed", type=int, default=7)
    run.add_argument("--trials", type=int, default=3)
    run.add_argument("--limits", type=Path, required=True, help="JSON object with all eight NativeChunkLimits fields.")
    run.add_argument("--adapter", choices=("baseline", "candidate"), default="candidate")
    run.add_argument("--strategy", choices=("full_refresh", "partition_replace"), default="full_refresh")
    run.add_argument("--mode", choices=("bounded_native", "isolated_switch"), default="bounded_native")
    run.add_argument("--factory", help="Reviewed DDA-06 module:callable; imported only after approval.")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--overwrite", action="store_true")
    inspect = commands.add_parser(
        "inspect", help="Verify retained evidence and list eligible trial count without services."
    )
    inspect.add_argument("report", type=Path)
    for command in ("recover", "cleanup"):
        operation = commands.add_parser(
            command, help="Reattach an existing owned invocation; never provision new objects."
        )
        operation.add_argument("--invocation-id", required=True)
        operation.add_argument("--factory", required=True)
        operation.add_argument("--limits", type=Path, required=True)
        operation.add_argument("--strategy", choices=("full_refresh", "partition_replace"), default="full_refresh")
        operation.add_argument("--mode", choices=("bounded_native", "isolated_switch"), default="bounded_native")
        operation.add_argument("--output", type=Path, required=True)
        operation.add_argument("--overwrite", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "inspect":
            envelope = json.loads(args.report.read_text(encoding="utf-8"))
            eligible = validate_run(envelope, args.report.parent)
            print(f"{args.report}: {envelope['status']}; eligible_trials={len(eligible)}")
            return 1 if envelope["status"] == "FAIL" else 0
        config = configuration(json.loads(args.limits.read_text(encoding="utf-8")))
        route = route_record(args.strategy, args.mode)
        store = ArtifactStore(args.output, overwrite=args.overwrite)
        store.preflight()
        if args.command in {"recover", "cleanup"}:
            owner = invocation_id(args.invocation_id)
            if not approved():
                result = {
                    "schema_version": 1,
                    "kind": "native-delivery-maintenance",
                    "status": "SKIP",
                    "reason": "disposable_environment_not_approved",
                }
                store.publish(result)
            else:
                result = maintain(
                    load_factory(args.factory, configuration=config, route=route),
                    action=args.command,
                    owner=owner,
                    store=store,
                )
            print(f"{args.output}: {result['status']}")
            return 1 if result["status"] == "FAIL" else 0
        dataset = Dataset(args.profile, args.rows, args.seed)
        if not 3 <= args.trials <= 100:
            raise ValueError("invalid_trials")
        reason = (
            "disposable_environment_not_approved"
            if not approved()
            else "real_route_factory_unavailable"
            if not args.factory
            else None
        )
        if reason:
            envelope = absent_run(store, dataset, config, route, git_identity(loaded_subject()), reason)
        else:
            adapter = ExecutionAdapter(load_factory(args.factory, configuration=config, route=route), args.adapter)
            envelope = run_benchmark(
                adapter=adapter, dataset=dataset, config=config, route=route, store=store, trials=args.trials
            )
        print(f"{args.output}: {envelope['status']}")
        return 1 if envelope["status"] == "FAIL" else 0
    except Exception:
        # Connector exceptions can contain credentials, SQL and row values.
        print(
            "native_delivery.invalid_input_or_execution; inspect configuration, factory and retained evidence",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
