"""Thread-safe real-vendor observation recorder for reviewed route cases."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import threading
from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from .campaign import CampaignContext
from .contract import CertificationValidationError, fail
from .evidence_writer import PassedCaseObservation, SuiteEvidenceWriter
from .registry import release_suites
from .reviewed_case import ReviewedCase, ReviewedSuite, canonical_json

WriterFactory = Callable[[ReviewedSuite], SuiteEvidenceWriter]


@dataclasses.dataclass(frozen=True, slots=True)
class ObservedImageDigest:
    """SHA-256 produced directly from a real-vendor image assertion."""

    sha256: str

    def __post_init__(self) -> None:
        from .contract import SHA256_PATTERN

        if not isinstance(self.sha256, str) or SHA256_PATTERN.fullmatch(self.sha256) is None:
            fail("recorder.observed_image_digest_invalid")


class RouteLiveObservationRecorder:
    """Collect exact observations and publish only a complete reviewed campaign."""

    def __init__(
        self,
        *,
        environment: Mapping[str, str],
        suites: Iterable[ReviewedSuite] | None = None,
        writer_factory: WriterFactory | None = None,
    ) -> None:
        self._environment = environment
        reviewed = tuple(release_suites() if suites is None else suites)
        if not reviewed:
            fail("recorder.suites_required")
        if len(reviewed) != len({suite.suite_id for suite in reviewed}):
            fail("recorder.suite_duplicate")
        self._suites = {suite.suite_id: suite for suite in sorted(reviewed, key=lambda item: item.suite_id)}
        self._cases = {
            suite_id: {case.case_id: case for case in suite.cases} for suite_id, suite in self._suites.items()
        }
        self._parameter_cases = {
            suite_id: {_parameters_json(case): case for case in suite.cases} for suite_id, suite in self._suites.items()
        }
        self._observations: dict[str, dict[str, PassedCaseObservation]] = {suite_id: {} for suite_id in self._suites}
        self._lock = threading.Lock()
        self._sealed = False
        self._writer_factory = writer_factory

    @property
    def authoritative(self) -> bool:
        """Whether this pytest session must publish the closed campaign."""

        return bool(self._environment.get("DPONE_ROUTE_LIVE_INVENTORY"))

    def observe_case(
        self,
        suite_id: str,
        case_id: str,
        *,
        before_image: object,
        after_image: object,
        observations: Mapping[str, Any],
    ) -> None:
        """Record one reviewed case after its real-vendor assertions passed."""

        reviewed = self._reviewed_case(suite_id, case_id)
        self._record(
            suite_id,
            reviewed,
            before_image=before_image,
            after_image=after_image,
            observations=observations,
        )

    def observe_parameters(
        self,
        suite_id: str,
        parameters: Mapping[str, Any],
        *,
        before_image: object,
        after_image: object,
        observations: Mapping[str, Any],
    ) -> None:
        """Resolve an authored parameter document, then record its exact case."""

        cases = self._parameter_cases.get(suite_id)
        if cases is None:
            fail(f"recorder.suite_not_reviewed:{suite_id}")
        reviewed = cases.get(canonical_json(dict(parameters)))
        if reviewed is None:
            fail(f"recorder.parameters_not_reviewed:{suite_id}")
        self._record(
            suite_id,
            reviewed,
            before_image=before_image,
            after_image=after_image,
            observations=observations,
        )

    def observe_case_digests(
        self,
        suite_id: str,
        case_id: str,
        *,
        before_image: ObservedImageDigest,
        after_image: ObservedImageDigest,
        observations: Mapping[str, Any],
    ) -> None:
        """Record hashes already computed by real-vendor snapshot assertions."""

        if not isinstance(before_image, ObservedImageDigest) or not isinstance(after_image, ObservedImageDigest):
            fail("recorder.observed_image_digest_required")
        reviewed = self._reviewed_case(suite_id, case_id)
        self._record_digests(
            suite_id,
            reviewed,
            before_sha256=before_image.sha256,
            after_sha256=after_image.sha256,
            observations=observations,
        )

    def write_complete_authority(self) -> tuple[tuple[Path, Path], ...]:
        """Reject every gap, then write all suite pairs in canonical order."""

        if not self.authoritative:
            return ()
        with self._lock:
            self._sealed = True
            frozen = {
                suite_id: tuple(values[case.case_id] for case in suite.cases if case.case_id in values)
                for suite_id, suite in self._suites.items()
                for values in (self._observations[suite_id],)
            }
            gaps = {
                suite_id: [case.case_id for case in self._suites[suite_id].cases if case.case_id not in values]
                for suite_id, values in self._observations.items()
                if len(values) != len(self._suites[suite_id].cases)
            }
        if gaps:
            summary = ",".join(f"{suite_id}:{len(case_ids)}" for suite_id, case_ids in sorted(gaps.items()))
            raise CertificationValidationError(f"recorder.campaign_incomplete:{summary}")
        writer_factory = self._writer_factory
        if writer_factory is None:
            writer_factory = CampaignContext.from_environment(self._environment).writer
        return tuple(
            writer_factory(self._suites[suite_id]).write(frozen[suite_id]) for suite_id in sorted(self._suites)
        )

    def coverage(self) -> dict[str, tuple[int, int]]:
        """Return observed and reviewed counts for diagnostics and tests."""

        with self._lock:
            return {
                suite_id: (len(self._observations[suite_id]), len(suite.cases))
                for suite_id, suite in self._suites.items()
            }

    def _reviewed_case(self, suite_id: str, case_id: str) -> ReviewedCase:
        suite = self._cases.get(suite_id)
        if suite is None:
            fail(f"recorder.suite_not_reviewed:{suite_id}")
        reviewed = suite.get(case_id)
        if reviewed is None:
            fail(f"recorder.case_not_reviewed:{suite_id}:{case_id}")
        return reviewed

    def _record(
        self,
        suite_id: str,
        reviewed: ReviewedCase,
        *,
        before_image: object,
        after_image: object,
        observations: Mapping[str, Any],
    ) -> None:
        self._record_digests(
            suite_id,
            reviewed,
            before_sha256=observed_image_sha256(before_image),
            after_sha256=observed_image_sha256(after_image),
            observations=observations,
        )

    def _record_digests(
        self,
        suite_id: str,
        reviewed: ReviewedCase,
        *,
        before_sha256: str,
        after_sha256: str,
        observations: Mapping[str, Any],
    ) -> None:
        value = PassedCaseObservation(
            reviewed.case_id,
            before_sha256,
            after_sha256,
            _jsonable_mapping(observations),
        )
        changed = value.before_image_sha256 != value.after_image_sha256
        if changed is not reviewed.expected_mutation:
            fail(f"recorder.case_mutation_mismatch:{suite_id}:{reviewed.case_id}")
        with self._lock:
            if self._sealed:
                fail("recorder.campaign_sealed")
            prior = self._observations[suite_id].get(reviewed.case_id)
            if prior is not None and prior != value:
                fail(f"recorder.case_conflict:{suite_id}:{reviewed.case_id}")
            self._observations[suite_id][reviewed.case_id] = value


def observed_image_sha256(value: object) -> str:
    """Hash a deterministic, secret-free representation of an observed image."""

    try:
        normalized = _jsonable(value)
    except TypeError as exc:
        raise CertificationValidationError("recorder.observed_image_not_serializable") from exc
    payload = json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _parameters_json(case: ReviewedCase) -> str:
    payload = json.loads(case.config_json)
    return canonical_json(payload["parameters"])


def _jsonable_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = _jsonable(value)
    if not isinstance(normalized, dict):  # pragma: no cover - Mapping invariant.
        raise TypeError("normalized observations must be a mapping")
    return normalized


def _jsonable(value: object) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            return {"__float__": repr(value)}
        return value
    if isinstance(value, Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, bytes):
        return {"__bytes_base64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (datetime, date, time)):
        return {"__temporal__": value.isoformat()}
    if isinstance(value, UUID):
        return {"__uuid__": str(value)}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _jsonable(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_jsonable(item) for item in value]
        return sorted(normalized, key=canonical_json)
    raise TypeError(f"unsupported observed value type: {type(value).__name__}")


__all__ = [
    "ObservedImageDigest",
    "RouteLiveObservationRecorder",
    "WriterFactory",
    "observed_image_sha256",
]
