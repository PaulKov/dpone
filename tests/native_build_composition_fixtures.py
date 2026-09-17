"""Compose real runtime services; SQL artifacts and original authority are synthetic.

These fixtures establish no route qualification, credentials, or live SQL proof.
"""

import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from dpone.adapters.dbt_artifacts import LocalDbtExecutionEvidenceWriter, LocalDbtRunResultsReader
from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
from dpone.adapters.native_dbt_profile_lease import NativeDbtProfileLease
from dpone.adapters.native_generation_invocation_auth import InvocationOriginalReader
from dpone.app.native_generation_bound_build import BoundNativeGenerationBuild
from dpone.contracts.dbt_contract_validation import artifact_json_bytes
from dpone.contracts.dbt_runtime import dbt_attempt_id
from dpone.contracts.native_delivery import GenerationReservation
from dpone.contracts.native_generation_invocation import encode_trusted_dbt_command_plan
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_trusted_dbt_environment_codec import (
    decode_trusted_dbt_owned_root,
    decode_trusted_dbt_qualification,
    encode_trusted_dbt_qualification,
)
from dpone.runtime.dbt_execution_policy import (
    build_dbt_command,
    build_dbt_ls_command,
    build_dbt_parse_command,
    prepare_dbt_build_output_paths,
    prepare_dbt_output_paths,
)
from dpone.runtime.native_generation_build_artifacts import CapturedBuildArtifactReader
from dpone.services.native_generation_build_bridge import ReservedDbtBuildBridge
from dpone.services.native_generation_build_evidence import NativeGenerationBuildEvidenceWriter
from dpone.services.native_generation_invocation_recorder import TrustedDbtInvocationRecorder
from tests.native_trusted_dbt_fixtures import InvocationFixture
from tests.test_dbt_runtime_execution import (
    _PROJECT_YAML,
    _attempt,
    _clock,
    _interval,
    _ManifestValidator,
    _preflight_manifest,
    _ProfileRenderer,
    _run_identity,
    _Runner,
    _ToolchainInspector,
)
from tests.test_native_generation_build_evidence import AcceptSchema
from tests.test_native_workspace_attempt_lifecycle import _fixture


class SyntheticBuildRunner(_Runner):
    """Emit the BUILD manifest omitted by the shared synthetic command runner."""

    def __init__(self, *, missing_manifest=False):
        super().__init__()
        self.missing_manifest = missing_manifest

    def run(self, args, *, cwd, timeout_seconds, redactions):
        result = super().run(args, cwd=cwd, timeout_seconds=timeout_seconds, redactions=redactions)
        if "build" in args and not self.missing_manifest:
            target = Path(args[args.index("--target-path") + 1])
            results = json.loads((target / "run_results.json").read_bytes())
            manifest = deepcopy(_preflight_manifest())
            manifest["metadata"]["invocation_id"] = results["metadata"]["invocation_id"]
            (target / "manifest.json").write_bytes(artifact_json_bytes(manifest))
        return result


def _bind_commands(invocation, pack, lease):
    """Rebind synthetic originals before constructing the actual recorder/writer."""
    output = invocation.target.parent
    paths = prepare_dbt_output_paths(
        output, pack.target_path, attempt_id=dbt_attempt_id(_run_identity(pack), _attempt())
    )
    prepare_dbt_build_output_paths(paths)
    argv = tuple(
        builder(
            pack,
            project_dir=invocation.project,
            profile_path=lease.profile_path,
            target_path=paths.target if index == 2 else paths.preflight_target,
            log_path=paths.logs if index == 2 else paths.preflight_logs,
            interval_vars_json=_interval().dbt_vars_json(),
        )
        for index, builder in enumerate((build_dbt_parse_command, build_dbt_ls_command, build_dbt_command))
    )
    replacements = {str(invocation.project): "PROJECT_DIR", str(lease.profile_path.parent): "PROFILE_DIR"}
    entries = tuple(
        replace(
            entry,
            argv_template=tuple(replacements.get(value, value) for value in args),
            command_timeout_seconds=timeout,
        )
        for entry, args, timeout in zip(invocation.plan.commands, argv, (120, 120, 600), strict=True)
    )
    invocation.plan = replace(invocation.plan, commands=entries, total_termination_budget_seconds=846)
    store = invocation.store
    command = store.add(
        "trusted_dbt_command_plan_v1", "invocation/command", encode_trusted_dbt_command_plan(invocation.plan)
    )
    invocation.executor = replace(invocation.executor, command=command)
    qualification = decode_trusted_dbt_qualification(store.documents["invocation/qualification"])
    store.add(
        "trusted_dbt_qualification_v1",
        "invocation/qualification",
        encode_trusted_dbt_qualification(replace(qualification, command_plan=command)),
    )
    return paths, argv


@contextmanager
def concrete_build(tmp_path, *, missing_manifest=False):
    """Hold the real profile lease through actual positive cohort authentication."""
    invocation = InvocationFixture(tmp_path)
    pack, request, admission, reads, lifecycle = _fixture(tmp_path)
    runner = SyntheticBuildRunner(missing_manifest=missing_manifest)
    store = invocation.store

    def ref(locator):
        return OriginalRef(locator, store.bound[locator].payload_sha256)

    with NativeDbtProfileLease(invocation.profile.parent, max_bytes=4096) as lease:
        profile_path = lease.profile_path
        info = profile_path.parent.stat()
        identity = (info.st_dev, info.st_ino)

        def require_clean_profile():
            assert not profile_path.exists()
            current = profile_path.parent.stat()
            assert (current.st_dev, current.st_ino) == identity
            assert admission.states == ["RUNNING"]

        require_clean_profile()
        paths, argv = _bind_commands(invocation, pack, lease)
        invocation.recorder = TrustedDbtInvocationRecorder(
            delegate=runner,
            executor=invocation.executor,
            command=invocation.executor.command,
            toolchain=ref("invocation/toolchain"),
            qualification=ref("invocation/qualification"),
            publish_original=store.publish,
            clock=invocation.clock,
            monotonic_clock=lambda: invocation.now,
            originals=store,
            bindings=store,
            subject=store.subject,
            max_metadata_bytes=1048576,
        )
        pack_ref = store.add(
            "generation_execution_pack_v1", "build/execution-pack.json", artifact_json_bytes(pack.to_dict())
        )
        reader = InvocationOriginalReader(originals=store, bindings=store, subject=store.subject, max_bytes=1048576)
        publication_kinds, delegate_writes = [], []

        def publish(**kwargs):
            require_clean_profile()
            publication_kinds.append(kwargs["kind"])
            return store.publish(**kwargs)

        local_writer = LocalDbtExecutionEvidenceWriter(tmp_path / "evidence.json")

        class ObservingDelegate:
            """Assert credential cleanup before delegating to the local producer."""

            def write(self, evidence):
                require_clean_profile()
                delegate_writes.append(evidence.status)
                return local_writer.write(evidence)

        require_clean_profile()
        writer = NativeGenerationBuildEvidenceWriter(
            delegate=ObservingDelegate(),
            executor=invocation.executor,
            execution_pack=pack,
            execution_pack_ref=pack_ref,
            run_identity=_run_identity(pack),
            toolchain=ref("invocation/toolchain"),
            qualification=ref("invocation/qualification"),
            admission_reader=reader,
            generation_reader=reader,
            build_argv=argv[2],
            project_directory=invocation.project,
            output_root=paths.root,
            artifact_reader=CapturedBuildArtifactReader(
                root=paths.root,
                target=paths.target,
                identity=decode_trusted_dbt_owned_root(store.documents["roots/OUTPUT"]),
                max_bytes=1048576,
            ),
            require_termination=invocation.recorder.require_completion,
            publish_original=publish,
            manifest_validator=AcceptSchema(),
            results_validator=OfficialDbtRunResultsValidator(),
            max_artifact_bytes=1048576,
            max_metadata_bytes=1048576,
        )
        (invocation.project / "dbt_project.yml").write_text(_PROJECT_YAML)
        build = BoundNativeGenerationBuild(
            pack=pack,
            runtime_root=invocation.project,
            run_output_root=paths.root,
            run_identity=_run_identity(pack),
            airflow_attempt=_attempt(),
            interval=_interval(),
            toolchain_inspector=_ToolchainInspector(),
            profile_store=lease,
            run_results_reader=LocalDbtRunResultsReader(),
            run_results_validator=OfficialDbtRunResultsValidator(),
            manifest_validator=_ManifestValidator(),
            workspace_attempt_lifecycle=lifecycle,
            clock=_clock(),
        )
        bridge = ReservedDbtBuildBridge(
            build=build,
            command_runner=invocation.recorder,
            profile_renderer=_ProfileRenderer(expected_schema="base"),
            evidence_writer=writer,
            originals=store,
            bindings=store,
            subject=store.subject,
            publish_original=store.publish,
            invocation=invocation.recorder,
        )
        reservation = GenerationReservation(
            invocation.executor.generation_id, invocation.executor.guard_epoch, 1, invocation.executor.reservation
        )
        yield SimpleNamespace(
            bridge=bridge,
            reservation=reservation,
            invocation=invocation,
            writer=writer,
            admission=admission,
            reads=reads,
            request=request,
            runner=runner,
            publication_kinds=publication_kinds,
            delegate_writes=delegate_writes,
            profile_path=profile_path,
            require_clean_profile=require_clean_profile,
        )
