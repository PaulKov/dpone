"""Synthetic authorized original ports, not evidence of actual route qualification."""

from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256

from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.native_generation_invocation import encode_trusted_dbt_command_plan
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeOriginalBinding
from dpone.contracts.native_trusted_dbt_environment import (
    TrustedDbtOwnedRoot,
    TrustedDbtQualification,
    TrustedDbtToolchain,
)
from dpone.contracts.native_trusted_dbt_environment_codec import (
    encode_trusted_dbt_owned_root,
    encode_trusted_dbt_qualification,
    encode_trusted_dbt_toolchain,
)
from dpone.ports.dbt_publishing import DbtCommandResult
from tests.test_native_original_bindings import binding
from tests.test_native_trusted_dbt_invocation import command_plan


class SyntheticOriginals:
    def __init__(self):
        self.documents = {}
        self.bound = {}
        self.subject = binding().subject
        self.publications = 0
        self.lose_ack = False
        self.fail_reads = False

    def add(self, kind, locator, payload):
        digest = "sha256:" + sha256(payload).hexdigest()
        ref = OriginalRef(locator, digest)
        obj = replace(binding().object_ref, key=locator, sha256=digest, size_bytes=len(payload))
        self.documents[locator] = payload
        self.bound[locator] = NativeOriginalBinding(
            self.subject, kind, binding().storage_authority, obj, digest, locator
        )
        return ref

    def resolve(self, reference, *, expected_subject, expected_kind):
        assert expected_subject == self.subject
        return self.bound[reference.locator]

    def read(self, exactref, *, expected_subject, expected_kind, max_bytes):
        if self.fail_reads:
            raise OSError("synthetic unavailable original")
        assert expected_subject == self.subject
        return self.documents[exactref.key]

    def publish(self, *, kind, locator, payload, max_bytes):
        self.publications += 1
        ref = self.add(kind, locator, payload)
        if self.lose_ack:
            raise OSError("synthetic lost publication acknowledgement")
        return ref


class SyntheticRunner:
    def __init__(self):
        self.calls = []
        self.action = lambda: DbtCommandResult(0)

    def run(self, args, *, cwd, timeout_seconds, redactions):
        self.calls.append((args, cwd, timeout_seconds, redactions))
        return self.action()


class InvocationFixture:
    def __init__(
        self, tmp_path, phase="BUILD", *, delegate=None, timeout=10, termination=2, clock=None, monotonic_clock=None
    ):
        from dpone.services.native_generation_invocation_recorder import TrustedDbtInvocationRecorder

        self.store = SyntheticOriginals()
        self.delegate = SyntheticRunner() if delegate is None else delegate
        self.now = 100.0
        self.clock = clock or (lambda: datetime(2026, 9, 15, tzinfo=UTC))
        roots = {role: (tmp_path / role).resolve() for role in ("PROJECT", "OUTPUT", "PROFILE")}
        for path in roots.values():
            path.mkdir()
        self.project = roots["PROJECT"]
        self.profile = roots["PROFILE"] / "fresh-invocation"
        self.profile.mkdir()
        self.target = roots["OUTPUT"] / "target"
        self.logs = roots["OUTPUT"] / "logs"
        self.target.mkdir()
        self.logs.mkdir()
        plan = command_plan(phase)
        references = {}
        for role, path in roots.items():
            info = path.stat()
            root = TrustedDbtOwnedRoot(
                "dpone.trusted-dbt-owned-root.v1",
                role,
                plan.executor_invocation_id,
                role.lower(),
                info.st_dev,
                info.st_ino,
                plan.project_root if role == "PROJECT" else None,
            )
            references[role] = self.store.add(
                "trusted_dbt_owned_root_v1", "roots/" + role, encode_trusted_dbt_owned_root(root)
            )
        entries = tuple(
            replace(
                entry,
                command_timeout_seconds=timeout,
                termination_allowance_seconds=termination,
                argv_template=(
                    "dbt",
                    entry.verb,
                    "--project-dir",
                    "PROJECT_DIR",
                    "--profiles-dir",
                    "PROFILE_DIR",
                    "--target-path",
                    str(self.target),
                    "--log-path",
                    str(self.logs),
                ),
            )
            for entry in plan.commands
        )
        self.plan = replace(
            plan,
            commands=entries,
            project_root=references["PROJECT"],
            output_root=references["OUTPUT"],
            profile_root=references["PROFILE"],
            total_termination_budget_seconds=(timeout + termination) * len(entries),
        )
        command = self.store.add(
            "trusted_dbt_command_plan_v1", "invocation/command", encode_trusted_dbt_command_plan(self.plan)
        )
        from tests.test_native_generation_admission import executor

        self.executor = replace(executor(), command=command)
        toolchain = TrustedDbtToolchain(
            "dpone.trusted-dbt-toolchain.v1", DBT_SQLSERVER_1_12_CERTIFIED, self.executor.profile
        )
        toolchain_ref = self.store.add(
            "trusted_dbt_toolchain_v1", "invocation/toolchain", encode_trusted_dbt_toolchain(toolchain)
        )
        qualification = TrustedDbtQualification(
            "dpone.trusted-dbt-qualification.v1",
            "synthetic-unit-fixture-only",
            self.executor.profile,
            toolchain_ref,
            command,
            (self.executor.reservation,),
        )
        qualification_ref = self.store.add(
            "trusted_dbt_qualification_v1", "invocation/qualification", encode_trusted_dbt_qualification(qualification)
        )
        self.recorder = TrustedDbtInvocationRecorder(
            delegate=self.delegate,
            executor=self.executor,
            command=command,
            toolchain=toolchain_ref,
            qualification=qualification_ref,
            publish_original=self.store.publish,
            clock=self.clock,
            monotonic_clock=monotonic_clock or (lambda: self.now),
            originals=self.store,
            bindings=self.store,
            subject=self.store.subject,
            max_metadata_bytes=65536,
        )

    def args(self, position):
        values = {"PROJECT_DIR": str(self.project), "PROFILE_DIR": str(self.profile)}
        return tuple(values.get(value, value) for value in self.plan.commands[position].argv_template)

    def run(self, position):
        return self.recorder.run(
            self.args(position),
            cwd=self.project,
            timeout_seconds=self.plan.commands[position].command_timeout_seconds,
            redactions=("unit-secret",),
        )

    def complete(self):
        self.recorder.validate_before_credentials()
        for index in range(len(self.plan.commands)):
            self.run(index)
        return self.recorder.require_completion()
