"""Fail-closed local privacy review for source migration; never publishes code.

See docs/public-clean-migration.md for policy, review, and private receipt formats.
All imports, argument parsing and acquisitions execute inside the error boundary.
"""


def _execute(args, dependencies):
    from dataclasses import asdict

    (Budget, Scanner, load_policy, Git, receipts) = dependencies
    root = args.root.resolve(strict=True)
    identity = receipts.scanner_identity()
    raw_policy = receipts.read_json(args.policy, root, 1024**2)
    policy = load_policy(raw_policy)
    budget = Budget()
    scanner = Scanner(policy, budget)
    git = Git(root, budget)
    # Resolve the output before acquiring content, but never create a partial
    # success receipt. An existing receipt cannot be reused or overwritten.
    receipts.private_parent(args.receipt, root)
    if args.receipt.exists() or args.receipt.is_symlink():
        raise receipts.GateError("RECEIPT_EXISTS")
    binding, review, expected = _acquire(args, root, git, scanner, receipts)
    findings = [{"item": index, **asdict(value)} for index, value in enumerate(scanner.findings, 1)]
    remaining = (
        receipts.apply_review(review, binding, policy, identity, findings, _syntax_validator(git, binding, policy))
        if args.mode in {"candidate", "commit"}
        else findings
    )
    if receipts.read_json(args.policy, root, 1024**2) != raw_policy or receipts.scanner_identity() != identity:
        raise receipts.GateError("SCAN_INPUT_CHANGED")
    if args.mode == "candidate":
        from tools.agent_policy.public_clean_candidate import index_identity, parse_metadata

        if (
            binding["index_digest"] != index_identity(git)
            or binding["parent"] != git.run("rev-parse", "HEAD", limit=64).decode().strip()
            or binding["metadata_digest"]
            != receipts.digest(parse_metadata(receipts.read_json(args.metadata, root, 1024**2)))
            or (args.review and receipts.read_json(args.review, root) != review)
        ):
            raise receipts.GateError("CANDIDATE_CHANGED")
    if args.mode == "commit":
        if expected is None or expected["policy_digest"] != policy.digest or expected["scanner_identity"] != identity:
            raise receipts.GateError("RECEIPT_MISMATCH")
        if expected["binding"] != binding or expected["review_digest"] != receipts.digest(review):
            raise receipts.GateError("RECEIPT_MISMATCH")
        if receipts.read_json(args.candidate_receipt, root) != expected or remaining:
            raise receipts.GateError("RECEIPT_MISMATCH")
    budget.tick()
    status = "BLOCKED" if remaining else "SCAN_COMPLETE"
    if args.mode in {"candidate", "commit"} and not remaining:
        status = "PASS" if review is not None else "REVIEW_REQUIRED"
    counts = {
        "items": budget.items,
        "bytes": budget.bytes,
        "archive_members": budget.archive_members,
        "archive_bytes": budget.archive_bytes,
    }
    if expected is not None and (expected["counts"] != counts or expected["findings"] != findings):
        raise receipts.GateError("RECEIPT_MISMATCH")
    payload = {
        "schema": "dpone.public-clean.v1",
        "mode": args.mode,
        "status": status,
        "complete": True,
        "counts": counts,
        "binding": binding,
        "policy_digest": policy.digest,
        "scanner_identity": identity,
        "review_digest": receipts.digest(review) if review is not None else None,
        "review": review,
        "findings": findings,
    }
    # Locators and reviewer prose are private, but must not become a second
    # storage location for credential-shaped material found in those fields.
    from tools.agent_policy.public_clean_policy import validate_receipt_credentials

    validate_receipt_credentials(receipts.canonical(payload), budget)
    receipts.write_receipt(args.receipt, root, payload)
    return {
        "schema": payload["schema"],
        "mode": args.mode,
        "status": status,
        "complete": True,
        "counts": counts,
        "findings": [{"item": row["item"], "code": row["code"]} for row in remaining],
    }


def _syntax_validator(git, binding, policy):
    """Inject exact-tree acquisition; legacy reviews do not load syntax parsers."""

    def validate(entry):
        from tools.agent_policy.public_clean_candidate import review_blob
        from tools.agent_policy.public_clean_syntax import verify_non_credential_syntax

        raw = review_blob(git, binding["tree"], entry["label"], entry["content_digest"])
        git.budget.tick()
        verify_non_credential_syntax(entry, raw, policy)
        git.budget.tick()

    return validate


def _acquire(args, root, git, scanner, receipts):
    from tools.agent_policy.public_clean_candidate import (
        SHA,
        parse_commit,
        parse_metadata,
        scan_candidate,
        scan_commit_metadata,
        scan_tree,
    )

    review = expected = None
    if args.mode == "candidate":
        metadata = parse_metadata(receipts.read_json(args.metadata, root, 1024**2))
        binding = scan_candidate(git, scanner)
        if binding["parent"] != metadata["parent"]:
            raise receipts.GateError("PARENT_MISMATCH")
        scan_commit_metadata(scanner, metadata)
        binding.update(
            metadata_digest=receipts.digest(metadata),
            author_date=metadata["author_date"],
            committer_date=metadata["committer_date"],
        )
        review = receipts.read_json(args.review, root) if args.review else None
    elif args.mode == "commit":
        expected = receipts.read_json(args.candidate_receipt, root)
        receipts.strict_keys(
            expected,
            {
                "schema",
                "mode",
                "status",
                "complete",
                "counts",
                "binding",
                "policy_digest",
                "scanner_identity",
                "review_digest",
                "review",
                "findings",
            },
        )
        if (
            expected["schema"] != "dpone.public-clean.v1"
            or expected["mode"] != "candidate"
            or expected["status"] != "PASS"
            or expected["complete"] is not True
            or expected["review"] is None
            or not SHA.fullmatch(args.commit_sha)
        ):
            raise receipts.GateError("RECEIPT_INVALID")
        binding = dict(expected["binding"])
        receipts.strict_keys(
            binding, {"tree", "parent", "index_digest", "metadata_digest", "author_date", "committer_date"}
        )
        actual = parse_commit(git.run("cat-file", "commit", args.commit_sha, limit=1024**2))
        if any(actual[k] != binding[k] for k in ("tree", "parent")):
            raise receipts.GateError("COMMIT_MISMATCH")
        if receipts.digest(actual["metadata"]) != binding["metadata_digest"]:
            raise receipts.GateError("COMMIT_MISMATCH")
        if any(actual["metadata"][k] != binding[k] for k in ("author_date", "committer_date")):
            raise receipts.GateError("COMMIT_MISMATCH")
        scan_tree(git, scanner, actual["tree"])
        scan_commit_metadata(scanner, actual["metadata"])
        review = expected["review"]
    elif args.mode == "history":
        from tools.agent_policy.public_clean_history import scan_history

        binding = scan_history(git, scanner)
    elif args.mode == "source-delta":
        from tools.agent_policy.public_clean_history import scan_delta

        binding = scan_delta(git, scanner, args.commit_sha)
    else:
        from tools.agent_policy.public_clean_worktree import scan_worktree

        binding = scan_worktree(git, scanner)
    return binding, review, expected


def main(argv=None):
    error_type = None
    alarm_state = None
    try:
        import signal

        def timeout(_signal, _frame):
            raise TimeoutError("Public-clean scan deadline exceeded.")

        alarm_state = (signal.getsignal(signal.SIGALRM), signal.getitimer(signal.ITIMER_REAL))
        signal.signal(signal.SIGALRM, timeout)
        signal.setitimer(signal.ITIMER_REAL, 900)
        import argparse
        import json
        import sys
        from pathlib import Path

        # Direct script execution does not otherwise expose the repository tools
        # package. Disable bytecode before importing any project module.
        sys.dont_write_bytecode = True
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from tools.agent_policy import public_clean_receipts as receipts
        from tools.agent_policy.public_clean_candidate import Git
        from tools.agent_policy.public_clean_policy import Budget, Scanner, load_policy

        error_type = receipts.GateError
        arguments = list(sys.argv[1:] if argv is None else argv)
        names = ("candidate", "commit", "history", "source-delta", "worktree")
        if arguments in (["--help"], ["-h"]) or (
            len(arguments) == 2 and arguments[0] in names and arguments[1] in {"--help", "-h"}
        ):
            common = ["--root", "--policy", "--receipt"]
            options = {
                "candidate": common + ["--metadata", "[--review]"],
                "commit": common + ["--commit-sha", "--candidate-receipt"],
                "history": common,
                "source-delta": common + ["--commit-sha"],
                "worktree": common,
            }
            selected = options if len(arguments) == 1 else {arguments[0]: options[arguments[0]]}
            print(
                json.dumps(
                    {
                        "schema": "dpone.public-clean-help.v1",
                        "commands": selected,
                        "guide": "docs/public-clean-migration.md",
                        "receipts": "private external files; no overwrite",
                        "exit_codes": {
                            "0": "PASS or discovery SCAN_COMPLETE",
                            "2": "BLOCKED or REVIEW_REQUIRED",
                            "3": "UNABLE_TO_CERTIFY",
                        },
                    },
                    separators=(",", ":"),
                )
            )
            return 0

        class Parser(argparse.ArgumentParser):
            def error(self, _message):
                raise receipts.GateError("ARGUMENT_INVALID")

        parser = Parser(add_help=False)
        modes = parser.add_subparsers(dest="mode", required=True)
        for name in names:
            command = modes.add_parser(name, add_help=False)
            command.add_argument("--root", type=Path, required=True)
            command.add_argument("--policy", type=Path, required=True)
            command.add_argument("--receipt", type=Path, required=True)
            if name == "candidate":
                command.add_argument("--metadata", type=Path, required=True)
                command.add_argument("--review", type=Path)
            if name in {"commit", "source-delta"}:
                command.add_argument("--commit-sha", required=True)
            if name == "commit":
                command.add_argument("--candidate-receipt", type=Path, required=True)
        args = parser.parse_args(arguments)
        result = _execute(args, (Budget, Scanner, load_policy, Git, receipts))
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result["status"] in {"PASS", "SCAN_COMPLETE"} else 2
    except Exception as exc:
        # Only our fixed internal diagnostic code can leave the failure boundary.
        # Bootstrap failures and unknown exceptions never expose their text.
        code = (
            exc.code
            if error_type and isinstance(exc, error_type)
            else "SCAN_TIMEOUT"
            if isinstance(exc, TimeoutError)
            else "INPUT_UNAVAILABLE"
        )
        try:
            import json

            print(
                json.dumps(
                    {
                        "schema": "dpone.public-clean.v1",
                        "status": "UNABLE_TO_CERTIFY",
                        "complete": False,
                        "counts": {},
                        "findings": [{"item": 0, "code": code}],
                    },
                    separators=(",", ":"),
                )
            )
        except Exception:
            print('{"schema":"dpone.public-clean.v1","status":"UNABLE_TO_CERTIFY","complete":false}')
        return 3
    finally:
        if alarm_state is not None:
            signal.setitimer(signal.ITIMER_REAL, *alarm_state[1])
            signal.signal(signal.SIGALRM, alarm_state[0])


if __name__ == "__main__":
    raise SystemExit(main())
