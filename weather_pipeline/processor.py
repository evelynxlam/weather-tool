"""
pipeline/processor.py — Data cleaning, transformation & storage pipeline.

Stages
──────
  RAW  →  VALIDATE  →  CLEAN  →  ENRICH  →  STORE

Each stage is a pure function; they are composed by run_pipeline().
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .validator import (
    ForecastValidationResult,
    validate_current,
    validate_forecast,
)

logger = logging.getLogger("weather.processor")

WMO_CODES: dict[int, str] = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Foggy", 48: "Icy fog", 51: "Light drizzle", 53: "Moderate drizzle",
    55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain",
    65: "Heavy rain", 71: "Slight snow", 73: "Moderate snow",
    75: "Heavy snow", 80: "Slight showers", 81: "Moderate showers",
    82: "Violent showers", 95: "Thunderstorm", 99: "Thunderstorm + hail",
}


# ── Stage 1 — Clean & flatten ─────────────────────────────────────────────────

def clean_records(validation: ForecastValidationResult) -> list[dict]:
    """
    Convert validated raw rows into typed, application-level records.

    • Renames API field names to short, snake_case equivalents
    • Coerces weathercode from float → int
    • Attaches human-readable description
    • Injects fetch timestamp
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    out = []
    for row in validation.records:
        code = int(row["weathercode"]) if row["weathercode"] is not None else None
        out.append({
            "date":         row["date"],
            "fetched_at":   fetched_at,
            "description":  WMO_CODES.get(code, f"WMO {code}") if code is not None else "Unknown",
            "temp_max_c":   row["temperature_2m_max"],
            "temp_min_c":   row["temperature_2m_min"],
            "precip_mm":    row["precipitation_sum"],
            "wind_max_kph": row["windspeed_10m_max"],
            "weathercode":  code,
        })
    return out


# ── Stage 2 — Enrich ──────────────────────────────────────────────────────────

def _feels_like(temp_c: float | None, wind_kph: float | None) -> float | None:
    """
    Wind-chill (Celsius) using the Environment Canada formula.
    Valid for T ≤ 10 °C and wind > 4.8 km/h.
    Returns None when inputs are missing or formula doesn't apply.
    """
    if temp_c is None or wind_kph is None:
        return None
    if temp_c > 10 or wind_kph <= 4.8:
        return None
    v016 = wind_kph ** 0.16
    wc = 13.12 + 0.6215 * temp_c - 11.37 * v016 + 0.3965 * temp_c * v016
    return round(wc, 1)


def _heat_index(temp_c: float | None) -> str | None:
    """Broad heat-index category based on daily max."""
    if temp_c is None:
        return None
    if temp_c >= 40:  return "Extreme heat"
    if temp_c >= 32:  return "Very hot"
    if temp_c >= 25:  return "Warm"
    if temp_c >= 15:  return "Mild"
    if temp_c >= 5:   return "Cool"
    if temp_c >= -5:  return "Cold"
    return "Freezing"


def enrich_records(records: list[dict]) -> list[dict]:
    """Add derived fields to each cleaned record."""
    for r in records:
        r["feels_like_c"]  = _feels_like(r["temp_min_c"], r["wind_max_kph"])
        r["heat_category"] = _heat_index(r["temp_max_c"])
        # temp range
        if r["temp_max_c"] is not None and r["temp_min_c"] is not None:
            r["temp_range_c"] = round(r["temp_max_c"] - r["temp_min_c"], 1)
        else:
            r["temp_range_c"] = None
    return records


# ── Stage 3 — Aggregate ───────────────────────────────────────────────────────

def aggregate(records: list[dict]) -> dict:
    """Compute period-level statistics from enriched records."""
    def vals(key):
        return [r[key] for r in records if r.get(key) is not None]

    def avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else None

    t_max = vals("temp_max_c")
    t_min = vals("temp_min_c")
    rain  = vals("precip_mm")
    wind  = vals("wind_max_kph")
    cats  = [r["heat_category"] for r in records if r.get("heat_category")]

    from collections import Counter
    dominant_cat = Counter(cats).most_common(1)[0][0] if cats else None

    return {
        "days_covered":        len(records),
        "avg_high_c":          avg(t_max),
        "avg_low_c":           avg(t_min),
        "max_high_c":          max(t_max, default=None),
        "min_low_c":           min(t_min, default=None),
        "total_precip_mm":     round(sum(rain), 2) if rain else None,
        "rainy_days":          sum(1 for r in rain if r > 1.0),
        "avg_wind_kph":        avg(wind),
        "peak_wind_kph":       max(wind, default=None),
        "dominant_heat_cat":   dominant_cat,
        "data_completeness":   round(
            sum(1 for r in records
                if r["temp_max_c"] is not None and r["temp_min_c"] is not None)
            / len(records) * 100, 1
        ) if records else 0,
    }


# ── Stage 4 — Store ───────────────────────────────────────────────────────────

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS forecasts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    location_name TEXT    NOT NULL,
    lat           REAL    NOT NULL,
    lon           REAL    NOT NULL,
    date          TEXT    NOT NULL,
    fetched_at    TEXT    NOT NULL,
    description   TEXT,
    temp_max_c    REAL,
    temp_min_c    REAL,
    precip_mm     REAL,
    wind_max_kph  REAL,
    weathercode   INTEGER,
    feels_like_c  REAL,
    heat_category TEXT,
    temp_range_c  REAL,
    UNIQUE(location_name, date, fetched_at)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at        TEXT NOT NULL,
    location_name TEXT NOT NULL,
    days_fetched  INTEGER,
    warnings      TEXT,   -- JSON array
    errors        TEXT,   -- JSON array
    latency_ms    REAL,
    records_stored INTEGER
);
"""


@contextmanager
def _db(path: Path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path):
    with _db(db_path) as conn:
        conn.executescript(DB_SCHEMA)
    logger.debug("Database initialised at %s", db_path)


def store_records(
    records: list[dict],
    location: dict,
    db_path: Path,
) -> int:
    """
    Upsert enriched records into SQLite.  Returns number of rows written.
    Uses INSERT OR IGNORE to skip exact duplicates (same location+date+fetch).
    """
    rows = [
        (
            location["name"], location["lat"], location["lon"],
            r["date"], r["fetched_at"], r["description"],
            r["temp_max_c"], r["temp_min_c"], r["precip_mm"],
            r["wind_max_kph"], r["weathercode"],
            r["feels_like_c"], r["heat_category"], r["temp_range_c"],
        )
        for r in records
    ]
    sql = """
        INSERT OR IGNORE INTO forecasts (
            location_name, lat, lon, date, fetched_at, description,
            temp_max_c, temp_min_c, precip_mm, wind_max_kph, weathercode,
            feels_like_c, heat_category, temp_range_c
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    with _db(db_path) as conn:
        conn.executemany(sql, rows)
        stored = conn.execute("SELECT changes()").fetchone()[0]
    logger.debug("Stored %d/%d records (duplicates skipped)", stored, len(rows))
    return stored


def log_run(
    db_path: Path,
    location_name: str,
    days_fetched: int,
    warnings: list[str],
    errors: list[str],
    latency_ms: float,
    records_stored: int,
):
    """Append a pipeline-run audit record."""
    with _db(db_path) as conn:
        conn.execute(
            """INSERT INTO pipeline_runs
               (run_at, location_name, days_fetched, warnings, errors,
                latency_ms, records_stored)
               VALUES (?,?,?,?,?,?,?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                location_name, days_fetched,
                json.dumps(warnings), json.dumps(errors),
                round(latency_ms, 1), records_stored,
            ),
        )


# ── Orchestrator ──────────────────────────────────────────────────────────────

@dataclass
class PipelineResult:
    location:       dict
    records:        list[dict]          = field(default_factory=list)
    current:        dict                = field(default_factory=dict)
    summary:        dict                = field(default_factory=dict)
    warnings:       list[str]           = field(default_factory=list)
    errors:         list[str]           = field(default_factory=list)
    records_stored: int                 = 0
    latency_ms:     float               = 0.0
    ok:             bool                = True

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)


def run_pipeline(
    raw_response: dict,
    location: dict,
    db_path: Path | None = None,
    t_start: float | None = None,
) -> PipelineResult:
    """
    Orchestrate all pipeline stages and return a PipelineResult.

    raw_response — dict straight from the API
    location     — {name, lat, lon, ...}
    db_path      — if provided, records are stored in SQLite
    t_start      — monotonic start time for latency calculation
    """
    import time
    t_start = t_start or time.monotonic()

    result = PipelineResult(location=location)

    # ── Validate ───────────────────────────────────────────────────────────────
    validation = validate_forecast(raw_response)
    result.warnings.extend(validation.warnings)
    result.errors.extend(validation.errors)

    if not validation.is_valid:
        result.ok = False
        logger.error("Validation failed:\n%s", "\n".join(validation.errors))
        return result

    if validation.warnings:
        logger.warning("%d validation warnings", len(validation.warnings))
        for w in validation.warnings:
            logger.warning("  ⚠  %s", w)

    # ── Clean ──────────────────────────────────────────────────────────────────
    records = clean_records(validation)
    logger.debug("Cleaned %d records", len(records))

    # ── Enrich ────────────────────────────────────────────────────────────────
    records = enrich_records(records)

    # ── Current weather ────────────────────────────────────────────────────────
    raw_current = raw_response.get("current_weather", {})
    current, cw_warnings = validate_current(raw_current)
    result.warnings.extend(cw_warnings)

    # ── Aggregate ──────────────────────────────────────────────────────────────
    summary = aggregate(records)

    # ── Store ──────────────────────────────────────────────────────────────────
    if db_path:
        db_path = Path(db_path)
        init_db(db_path)
        stored = store_records(records, location, db_path)
        result.records_stored = stored
        latency_ms = (time.monotonic() - t_start) * 1000
        log_run(
            db_path, location["name"], len(records),
            result.warnings, result.errors, latency_ms, stored,
        )

    result.records   = records
    result.current   = current
    result.summary   = summary
    result.latency_ms = (time.monotonic() - t_start) * 1000
    return result
