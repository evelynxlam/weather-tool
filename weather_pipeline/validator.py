"""
pipeline/validator.py — Schema validation & field-level sanitisation.

Every raw API response passes through validate_forecast() and
validate_current() before entering the processing stage.  Failures
raise ValidationError with a structured report so callers can decide
whether to abort, retry, or continue with partial data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


# ── Custom exception ───────────────────────────────────────────────────────────

class ValidationError(Exception):
    """Raised when a response fails schema validation."""

    def __init__(self, message: str, errors: list[str]):
        super().__init__(message)
        self.errors = errors

    def __str__(self):
        lines = [super().__str__()]
        for e in self.errors:
            lines.append(f"  • {e}")
        return "\n".join(lines)


# ── Field-level sanitisers ─────────────────────────────────────────────────────

def _clean_float(value: Any, name: str, lo: float, hi: float) -> float | None:
    """
    Return a clean float or None.
    - None / NaN / Inf → None (missing data, not an error)
    - Out-of-range      → None + warning logged inside return tuple
    Returns (cleaned_value, warning_message_or_None).
    """
    if value is None:
        return None, None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None, f"{name}: cannot coerce {value!r} to float"
    if math.isnan(v) or math.isinf(v):
        return None, f"{name}: non-finite value {v}"
    if not (lo <= v <= hi):
        return None, f"{name}: {v} outside expected range [{lo}, {hi}]"
    return v, None


def _clean_date(value: Any, name: str = "date") -> str | None:
    """Expect ISO-8601 date string YYYY-MM-DD."""
    if not isinstance(value, str):
        return None, f"{name}: expected string, got {type(value).__name__}"
    import re
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return None, f"{name}: {value!r} is not YYYY-MM-DD"
    return value, None


# ── Forecast response schema ───────────────────────────────────────────────────

# Expected daily arrays and their physical plausibility bounds
DAILY_FIELDS: dict[str, tuple[float, float]] = {
    "temperature_2m_max":  (-90.0,  60.0),   # °C  — world extremes
    "temperature_2m_min":  (-90.0,  60.0),
    "precipitation_sum":   (  0.0, 500.0),   # mm/day — Cherrapunji record ~380 mm
    "windspeed_10m_max":   (  0.0, 450.0),   # km/h  — tornado upper bound
    "weathercode":         (  0.0,  99.0),   # WMO code
}


@dataclass
class ForecastValidationResult:
    is_valid:   bool = True
    errors:     list[str] = field(default_factory=list)
    warnings:   list[str] = field(default_factory=list)
    # Cleaned records are written here so callers get one object back
    records:    list[dict] = field(default_factory=list)


def validate_forecast(raw: dict) -> ForecastValidationResult:
    """
    Validate and clean a raw Open-Meteo forecast response.

    Hard errors  → result.is_valid = False  (caller should not proceed)
    Soft warnings → result.warnings         (caller may proceed with NULLs)
    """
    result = ForecastValidationResult()

    # ── Top-level structure ────────────────────────────────────────────────────
    if not isinstance(raw, dict):
        result.is_valid = False
        result.errors.append(f"Response is not a JSON object (got {type(raw).__name__})")
        return result

    if "daily" not in raw:
        result.is_valid = False
        result.errors.append("Missing top-level 'daily' key")
        return result

    daily = raw["daily"]
    if "time" not in daily:
        result.is_valid = False
        result.errors.append("daily.time array is missing")
        return result

    n = len(daily["time"])
    if n == 0:
        result.is_valid = False
        result.errors.append("daily.time is empty — no forecast days returned")
        return result

    # ── Array-length consistency ───────────────────────────────────────────────
    for key in DAILY_FIELDS:
        if key not in daily:
            result.errors.append(f"daily.{key} array is absent — will use NULLs")
            daily[key] = [None] * n          # patch so downstream loops work
        elif len(daily[key]) != n:
            result.errors.append(
                f"daily.{key} length {len(daily[key])} ≠ time length {n}"
            )
            # Pad/truncate to n so we don't crash later
            arr = daily[key]
            daily[key] = (arr + [None] * n)[:n]

    if result.errors:
        result.is_valid = False

    # ── Per-row cleaning ───────────────────────────────────────────────────────
    WMO_VALID = set(range(0, 4)) | {45, 48} | set(range(51, 56)) | \
                set(range(61, 66)) | set(range(71, 76)) | \
                set(range(80, 83)) | {95, 99}

    for i, raw_date in enumerate(daily["time"]):
        date, warn = _clean_date(raw_date)
        if warn:
            result.warnings.append(f"Row {i} {warn}")
            date = f"row-{i}"

        cleaned: dict[str, Any] = {"date": date}

        for col, (lo, hi) in DAILY_FIELDS.items():
            val, warn = _clean_float(daily[col][i], f"row {i} {col}", lo, hi)
            if warn:
                result.warnings.append(warn)
            cleaned[col] = val

        # WMO code sanity
        code = cleaned.get("weathercode")
        if code is not None and int(code) not in WMO_VALID:
            result.warnings.append(
                f"Row {i}: weathercode {int(code)} is not a recognised WMO code"
            )

        result.records.append(cleaned)

    return result


# ── Current-weather schema ─────────────────────────────────────────────────────

CURRENT_FIELDS: dict[str, tuple[float, float]] = {
    "temperature":    (-90.0, 60.0),
    "windspeed":      (  0.0, 450.0),
    "winddirection":  (  0.0, 360.0),
}


def validate_current(raw_current: dict) -> tuple[dict, list[str]]:
    """
    Validate current_weather block.  Returns (cleaned_dict, warnings).
    Never raises — missing current weather is non-fatal.
    """
    warnings: list[str] = []
    cleaned: dict = {}

    for key, (lo, hi) in CURRENT_FIELDS.items():
        val, warn = _clean_float(raw_current.get(key), key, lo, hi)
        if warn:
            warnings.append(f"current_weather.{warn}")
        cleaned[key] = val

    cleaned["is_day"] = bool(raw_current.get("is_day", 1))
    return cleaned, warnings
