"""Concrete authenticated B1 producer; application/recorder admission is required.

This preparatory adapter is not wired to managed execution. In particular the
legacy profile renderer/policy do not yet select the new adapter identity.
"""

import os
from collections.abc import Callable
from pathlib import Path
from threading import Event, Lock
from time import monotonic_ns
from typing import cast
from uuid import uuid4

from dpone.adapters.dbt_mssql_physical_catalog_policy import MssqlPhysicalCatalogPolicyReader
from dpone.adapters.dbt_mssql_physical_registration_store import MssqlPhysicalRegistrationStore
from dpone.adapters.dbt_physical_transport_profile import PhysicalTransportProfile
from dpone.adapters.dbt_physical_transport_subprocess import PhysicalTransportLaunchLease
from dpone.adapters.native_generation_invocation_auth import (
    InvocationOriginalReader,
    authenticate_invocation,
    verify_invocation_paths,
)
from dpone.contracts.dbt_mssql_physical_invocation import PhysicalManagedInvocation, require_managed_command
from dpone.contracts.dbt_mssql_physical_registration import (
    MssqlPhysicalRuntimeRegistration,
    database_pin_payload,
    physical_runtime_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_registration_values import platform_subject_payload, reference_payload
from dpone.contracts.dbt_mssql_physical_wire import PHYSICAL_PLAN_SET_KIND, decode_physical_plan_set
from dpone.contracts.dbt_physical_transport_delivery import (
    DELIVERY_SCHEMA,
    PhysicalTransportDelivery,
    physical_transport_argv_digest,
)
from dpone.contracts.native_delivery import NativeOriginalsRefV1
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_generation_invocation import AuthenticatedInvocationPlan, invocation_path_slots
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeOriginalKind
from dpone.contracts.native_source_custody import SourceExecutorBinding


class AuthenticatedPhysicalTransportDeliveryProducer:
    """Read real policy, retained SQL registration and generation-bound originals.

    Construct only at the native application bootstrap. Call prepare_command
    inside the recorder's consumed admission slot with its original absolute D
    and cancellation Event. This object does not invent/extend that deadline or
    admit commands itself. Each position is consumed even when preparation fails.
    """

    def __init__(
        self,
        *,
        policy_reader: MssqlPhysicalCatalogPolicyReader,
        registration_store: MssqlPhysicalRegistrationStore,
        originals: InvocationOriginalReader,
        refs: NativeOriginalsRefV1,
        registration: MssqlPhysicalRuntimeRegistration,
        executor: SourceExecutorBinding,
        toolchain: OriginalRef,
        qualification: OriginalRef,
        plan_set: OriginalRef,
        profile: PhysicalTransportProfile,
        cancellation: Event,
        clock: Callable[[], int] = monotonic_ns,
    ) -> None:
        for value, expected in (
            (policy_reader, MssqlPhysicalCatalogPolicyReader),
            (registration_store, MssqlPhysicalRegistrationStore),
            (originals, InvocationOriginalReader),
            (refs, NativeOriginalsRefV1),
            (registration, MssqlPhysicalRuntimeRegistration),
            (executor, SourceExecutorBinding),
            (profile, PhysicalTransportProfile),
            (cancellation, Event),
            (toolchain, OriginalRef),
            (qualification, OriginalRef),
            (plan_set, OriginalRef),
        ):
            if type(value) is not expected:
                raise ValueError("physical delivery requires concrete authority owners and exact original identities")
        registration.__post_init__()
        originals.require_generation(executor)
        self._policy, self._store, self._reader = policy_reader, registration_store, originals
        self._refs, self._registration, self._executor = refs, registration, executor
        self._toolchain, self._qualification, self._plan_ref = toolchain, qualification, plan_set
        self._profile, self._cancel, self._clock = profile, cancellation, clock
        self._lock, self._positions = Lock(), set[int]()

    def prepare_command(
        self,
        authenticated: AuthenticatedInvocationPlan,
        command_index: int,
        args: tuple[str, ...],
        profile_file: Path,
        admitted_monotonic_ns: int,
        deadline_monotonic_ns: int,
    ) -> PhysicalTransportLaunchLease:
        """Return one spawn lease only after fresh independent actual readbacks."""
        with self._lock:
            if type(command_index) is not int or command_index < 0 or command_index in self._positions:
                raise ValueError("physical delivery command slot invalid or consumed")
            self._positions.add(command_index)
            if (
                type(admitted_monotonic_ns) is not int
                or type(deadline_monotonic_ns) is not int
                or not 0 < admitted_monotonic_ns <= self._clock() < deadline_monotonic_ns
                or self._cancel.is_set()
            ):
                raise ValueError("physical delivery admission deadline invalid or cancelled")
            actual = authenticate_invocation(
                self._reader,
                executor=self._executor,
                command=self._executor.command,
                toolchain=self._toolchain,
                qualification=self._qualification,
            )
            if type(authenticated) is not AuthenticatedInvocationPlan or authenticated != actual:
                raise ValueError("physical delivery differs from freshly authenticated invocation")
            if not 0 <= command_index < len(actual.plan.commands):
                raise ValueError("physical delivery command index outside admitted membership")
            entry = actual.plan.commands[command_index]
            if deadline_monotonic_ns > admitted_monotonic_ns + entry.command_timeout_seconds * 1_000_000_000:
                raise ValueError("physical delivery exceeds the locked command deadline")
            slots = invocation_path_slots(actual, position=command_index, args=args)
            directory = verify_invocation_paths(
                actual,
                position=command_index,
                args=args,
                cwd=Path(args[slots["--project-dir"]]),
                previous_profile=profile_file.parent,
            )
            snapshot = self._profile.snapshot(profile_file)
            registration = self._store.resolve(self._registration)
            if registration != self._registration:
                raise ValueError("physical retained registration changed")
            policy = self._policy.read(self._refs, registration=registration)
            command = self._reader.read(self._executor.command, "trusted_dbt_command_plan_v1")
            require_managed_command(
                command,
                expected=PhysicalManagedInvocation(
                    str(self._executor.generation_id),
                    str(self._executor.invocation_id),
                    self._plan_ref,
                    registration.registration_id,
                ),
            )
            plan = decode_physical_plan_set(
                self._reader.read(self._plan_ref, cast(NativeOriginalKind, PHYSICAL_PLAN_SET_KIND))
            )
            if (
                plan.generation_id != str(self._executor.generation_id)
                or plan.runtime_registration_id != registration.registration_id
                or plan.profile != registration.trusted_profile.reference
                or plan.profile != self._executor.profile
                or plan.guard.fencing_epoch != self._executor.guard_epoch
                or plan.model_database != registration.model_database
                or registration.trusted_toolchain.reference != self._toolchain
                or policy.registration_sha256 != physical_runtime_registration_digest(registration)
                or (policy.profile_name, policy.model_database_name, policy.model_schema)
                != (snapshot.profile_name, snapshot.database, snapshot.schema)
                or policy.resource_bounds != plan.profile
                or any(model.spec.resource_bounds != policy.resource_bounds for model in plan.models)
                or any(
                    (model.spec.relation.database, model.spec.relation.schema) != (snapshot.database, snapshot.schema)
                    for model in plan.models
                )
            ):
                raise ValueError("physical delivery plan/policy/profile/executor scope differs")
            payload = encode_native_delivery_json(
                dict(
                    schema=DELIVERY_SCHEMA,
                    launch_id=str(uuid4()),
                    command_index=command_index,
                    parent_pid=os.getpid(),
                    admitted_monotonic_ns=admitted_monotonic_ns,
                    deadline_monotonic_ns=deadline_monotonic_ns,
                    generation_id=str(self._executor.generation_id),
                    executor_invocation_id=str(self._executor.invocation_id),
                    guard_epoch=self._executor.guard_epoch,
                    command_plan=reference_payload(self._executor.command),
                    toolchain=reference_payload(self._toolchain),
                    qualification=reference_payload(self._qualification),
                    registration_id=registration.registration_id,
                    registration_sha256=physical_runtime_registration_digest(registration),
                    platform_subject=platform_subject_payload(registration.platform_subject),
                    trusted_profile=registration.trusted_profile.to_dict(),
                    limits=registration.limits.to_dict(),
                    plan_set=reference_payload(self._plan_ref),
                    model_database=database_pin_payload(plan.model_database),
                    model_schema=snapshot.schema,
                    adapter_type="dpone_sqlserver",
                    profile_name=snapshot.profile_name,
                    target_name=snapshot.target_name,
                    argv_sha256=physical_transport_argv_digest(args),
                    profile_file_device=snapshot.device,
                    profile_file_inode=snapshot.inode,
                    profile_file_sha256=snapshot.sha256,
                    local_schema=registration.local_schema,
                )
            )
            if self._cancel.is_set() or self._clock() >= deadline_monotonic_ns:
                raise ValueError("physical delivery expired during authenticated preparation")
            return PhysicalTransportLaunchLease(
                delivery=PhysicalTransportDelivery(payload),
                profile_directory=directory,
                cancellation=self._cancel,
                clock=self._clock,
            )
