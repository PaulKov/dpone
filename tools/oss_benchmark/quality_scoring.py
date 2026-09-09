"""SOLID/Clean OOP proxy scoring for benchmark collectors."""

from __future__ import annotations

import re
from pathlib import Path

from tools.oss_benchmark.models import QualityScore, QualitySignal


def looks_interface_like(path: Path, text: str) -> bool:
    suffix = path.suffix.lower()
    if suffix == ".py":
        return "Protocol" in text or "ABC" in text or "@runtime_checkable" in text
    if suffix in {".java", ".kt", ".kts", ".groovy", ".scala", ".ts", ".tsx"}:
        return bool(re.search(r"\binterface\s+[A-Za-z_]\w*", text))
    return False


def score_quality(signal: QualitySignal) -> QualityScore:
    """Score static SOLID/Clean OOP signals on a 0-5 scale."""

    solid = 5.0
    clean = 5.0
    evidence: list[str] = []
    solid, clean = _score_module_size(signal, solid, clean, evidence)
    solid, clean = _score_coupling(signal, solid, clean, evidence)
    solid, clean = _score_cohesion(signal, solid, clean, evidence)
    solid = _score_interfaces(signal, solid, evidence)
    return QualityScore(
        solid=round(max(0.0, min(5.0, solid)), 1),
        clean_oop=round(max(0.0, min(5.0, clean)), 1),
        evidence=tuple(evidence),
    )


def _score_module_size(
    signal: QualitySignal,
    solid: float,
    clean: float,
    evidence: list[str],
) -> tuple[float, float]:
    if signal.max_module_loc > 1200:
        solid -= 0.8
        clean -= 1.0
        evidence.append(f"largest module is very large ({signal.max_module_loc} LOC)")
    elif signal.max_module_loc > 600:
        solid -= 0.4
        clean -= 0.5
        evidence.append(f"largest module exceeds 600 LOC ({signal.max_module_loc})")
    else:
        evidence.append(f"largest module stays within the 600 LOC target ({signal.max_module_loc})")
    if signal.p90_module_loc > 600:
        clean -= 0.8
        evidence.append(f"P90 module size is high ({signal.p90_module_loc:.0f} LOC)")
    elif signal.p90_module_loc <= 250:
        evidence.append(f"P90 module size is compact ({signal.p90_module_loc:.0f} LOC)")
    return solid, clean


def _score_coupling(
    signal: QualitySignal,
    solid: float,
    clean: float,
    evidence: list[str],
) -> tuple[float, float]:
    if signal.avg_ce > 12:
        solid -= 0.8
        clean -= 0.6
        evidence.append(f"average fan-out is high ({signal.avg_ce:.2f})")
    elif signal.avg_ce <= 6:
        evidence.append(f"average fan-out is controlled ({signal.avg_ce:.2f})")
    if signal.p90_ce > 18:
        solid -= 0.7
        clean -= 0.5
        evidence.append(f"P90 fan-out is high ({signal.p90_ce:.0f})")
    elif signal.p90_ce <= 12:
        evidence.append(f"P90 fan-out is controlled ({signal.p90_ce:.0f})")
    if signal.max_ce > 60:
        solid -= 0.7
        clean -= 0.4
        evidence.append(f"top module fan-out is very high ({signal.max_ce})")
    elif signal.max_ce <= 20:
        evidence.append(f"top module fan-out remains bounded ({signal.max_ce})")
    return solid, clean


def _score_cohesion(
    signal: QualitySignal,
    solid: float,
    clean: float,
    evidence: list[str],
) -> tuple[float, float]:
    if signal.avg_clustering > 0.25:
        solid -= 0.7
        clean -= 0.7
        evidence.append(f"average clustering indicates tight dependency triangles ({signal.avg_clustering:.3f})")
    elif signal.avg_clustering <= 0.18:
        evidence.append(f"average clustering is inside the green target ({signal.avg_clustering:.3f})")
    if signal.cohesion_ratio < 0.35:
        solid -= 0.7
        clean -= 0.5
        evidence.append(f"cohesion ratio is low ({signal.cohesion_ratio:.3f})")
    elif signal.cohesion_ratio >= 0.55:
        evidence.append(f"cohesion ratio is healthy ({signal.cohesion_ratio:.3f})")
    return solid, clean


def _score_interfaces(signal: QualitySignal, solid: float, evidence: list[str]) -> float:
    if signal.interface_density < 0.02:
        solid -= 0.4
        evidence.append(f"few explicit interface/protocol files ({signal.interface_density:.3f} density)")
    elif signal.interface_density >= 0.08:
        evidence.append(f"explicit interface/protocol density supports DIP/ISP ({signal.interface_density:.3f})")
    return solid
