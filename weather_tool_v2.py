#!/usr/bin/env python3
"""
weather_tool_v2.py — Production-grade weather CLI with full pipeline.

Usage
─────
  python weather_tool_v2.py --city "Denver" --days 7
  python weather_tool_v2.py --city "Tokyo"  --days 3 --export both
  python weather_tool_v2.py --city "London" --days 5 --db weather.db
  python weather_tool_v2.py --city "Paris"  --days 7 --ai-debug
  python weather_tool_v2.py --city "Sydney" --days 7 --ai-perf
  python weather_tool_v2.py --city "NYC"    --days 7 --verbose
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

# Ensure the folder containing this script is on sys.path so that the
# 'weather_pipeline' package is always found, regardless of where Python is launched from.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from weather_pipeline import (
    APIError, CircuitOpenError, RateLimitError,
    WeatherAPIClient,
    run_pipeline, PipelineResult,
    diagnose_error, analyse_performance, explain_warnings,
)

# ── API endpoints ──────────────────────────────────────────────────────────────

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL  = "https://api.open-meteo.com/v1/forecast"

# ── Terminal colours ──────────────────────────────────────────────────────────

CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GREY   = "\033[90m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


# ── Geocoding ─────────────────────────────────────────────────────────────────

def geocode(client: WeatherAPIClient, city: str) -> dict:
    data = client.get(
        GEOCODING_URL,
        params={"name": city, "count": 1, "language": "en", "format": "json"},
    )
    if not data.get("results"):
        raise ValueError(f"City not found: '{city}'")
    r = data["results"][0]
    return {
        "lat":    r["latitude"],
        "lon":    r["longitude"],
        "name":   r.get("name", city),
        "country":r.get("country", ""),
        "admin1": r.get("admin1", ""),
    }


# ── Forecast fetch ────────────────────────────────────────────────────────────

def fetch_forecast(client: WeatherAPIClient, lat: float, lon: float, days: int) -> dict:
    return client.get(
        FORECAST_URL,
        params={
            "latitude":       lat,
            "longitude":      lon,
            "daily":          [
                "temperature_2m_max", "temperature_2m_min",
                "precipitation_sum",  "windspeed_10m_max",
                "weathercode",
            ],
            "current_weather": True,
            "timezone":        "auto",
            "forecast_days":   min(days, 16),
        },
    )


# ── Display helpers ────────────────────────────────────────────────────────────

def temp_bar(value, lo=-10, hi=40, width=10):
    if value is None:
        return " " * width
    frac   = max(0, min(1, (value - lo) / (hi - lo)))
    filled = round(frac * width)
    return "█" * filled + "░" * (width - filled)


def print_report(result: PipelineResult):
    loc = result.location
    loc_str = loc["name"]
    if loc.get("admin1"):  loc_str += f", {loc['admin1']}"
    if loc.get("country"): loc_str += f", {loc['country']}"

    print(f"\n{BOLD}{CYAN}{'─'*62}{RESET}")
    print(f"  {BOLD}📍 {loc_str}{RESET}")
    print(f"  {CYAN}Lat {loc['lat']:.4f}  Lon {loc['lon']:.4f}{RESET}")
    print(f"  Pipeline latency: {result.latency_ms:.0f} ms")

    if result.has_warnings:
        print(f"  {YELLOW}⚠  {len(result.warnings)} warning(s) — run with --ai-warn for analysis{RESET}")
    print(f"{CYAN}{'─'*62}{RESET}")

    # Current weather
    cw = result.current
    if cw:
        print(f"\n  {BOLD}Current Conditions{RESET}")
        print(f"  Temperature : {YELLOW}{cw.get('temperature')} °C{RESET}")
        print(f"  Wind        : {cw.get('windspeed')} km/h @ {cw.get('winddirection')}°")
        print(f"  Time of day : {'Day ☀️' if cw.get('is_day') else 'Night 🌙'}")

    # Forecast table
    print(f"\n  {BOLD}Daily Forecast{RESET}")
    hdr = f"  {'Date':<12} {'Conditions':<22} {'High':>6} {'Low':>6} {'Rain':>7} {'Wind':>8}  {'Category':<14}  Bar"
    print(hdr)
    print(f"  {'─'*12} {'─'*22} {'─'*6} {'─'*6} {'─'*7} {'─'*8}  {'─'*14}  {'─'*10}")

    for r in result.records:
        def fmt(v, fmt_str): return fmt_str.format(v) if v is not None else "  N/A"
        bar = temp_bar(r["temp_max_c"])
        cat = (r.get("heat_category") or "")[:14]
        print(
            f"  {r['date']:<12} {r['description']:<22} "
            f"{GREEN}{fmt(r['temp_max_c'], '{:>5.1f}°')}{RESET} "
            f"{fmt(r['temp_min_c'], '{:>5.1f}°')} "
            f"{fmt(r['precip_mm'], '{:>6.1f}mm')} "
            f"{fmt(r['wind_max_kph'], '{:>7.1f}kph')}  "
            f"{cat:<14}  {bar}"
        )

    # Summary
    s = result.summary
    print(f"\n{CYAN}{'─'*62}{RESET}")
    print(f"  {BOLD}Period Summary ({s.get('days_covered')} days){RESET}")
    print(f"  Avg high      : {YELLOW}{s.get('avg_high_c')} °C{RESET}  (peak {s.get('max_high_c')} °C)")
    print(f"  Avg low       : {s.get('avg_low_c')} °C  (min {s.get('min_low_c')} °C)")
    print(f"  Total rain    : {s.get('total_precip_mm')} mm  ({s.get('rainy_days')} rainy days)")
    print(f"  Avg wind      : {s.get('avg_wind_kph')} kph  (peak {s.get('peak_wind_kph')} kph)")
    print(f"  Dominant feel : {s.get('dominant_heat_cat')}")
    print(f"  Data complete : {s.get('data_completeness')}%")
    if result.records_stored:
        print(f"  Stored to DB  : {result.records_stored} new records")
    print(f"{CYAN}{'─'*62}{RESET}\n")


# ── Export ────────────────────────────────────────────────────────────────────

def export_json(result: PipelineResult, path: str):
    payload = {
        "location": result.location,
        "current":  result.current,
        "forecast": result.records,
        "summary":  result.summary,
        "warnings": result.warnings,
    }
    Path(path).write_text(json.dumps(payload, indent=2))
    print(f"  ✅  JSON → {path}")


def export_csv(result: PipelineResult, path: str):
    if not result.records:
        print("  No records to export.")
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=result.records[0].keys())
        writer.writeheader()
        writer.writerows(result.records)
    print(f"  ✅  CSV  → {path}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Weather data pipeline v2 — validate, enrich, store & debug.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    loc = p.add_mutually_exclusive_group(required=True)
    loc.add_argument("--city", metavar="NAME")
    loc.add_argument("--lat",  type=float, metavar="LAT")
    p.add_argument("--lon",    type=float, metavar="LON")
    p.add_argument("--days",   type=int,   default=7, metavar="N",
                   help="Forecast days 1–16 (default 7)")
    p.add_argument("--export", choices=["json", "csv", "both"])
    p.add_argument("--out",    default="weather_output",
                   help="Base filename for exports")
    p.add_argument("--db",     metavar="PATH",
                   help="SQLite database path for persistent storage")
    p.add_argument("--ai-debug",  action="store_true",
                   help="On error: ask Claude to diagnose the issue")
    p.add_argument("--ai-perf",   action="store_true",
                   help="After run: ask Claude to analyse performance")
    p.add_argument("--ai-warn",   action="store_true",
                   help="After run: ask Claude to explain validation warnings")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Enable debug logging")
    return p.parse_args()


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
        level=level,
    )


def main():
    args = parse_args()
    setup_logging(args.verbose)

    with WeatherAPIClient() as client:
        t_start = time.monotonic()

        # ── Geocode ───────────────────────────────────────────────────────────
        if args.city:
            print(f"\n  🔍 Geocoding '{args.city}' …")
            try:
                location = geocode(client, args.city)
            except (APIError, ValueError) as exc:
                print(f"  {RED}❌  {exc}{RESET}")
                if args.ai_debug:
                    status = getattr(exc, "status_code", None)
                    url    = getattr(exc, "url", "")
                    report = diagnose_error(exc, status_code=status, url=url)
                    print(report)
                sys.exit(1)
        else:
            if args.lon is None:
                print(f"  {RED}❌  --lon is required with --lat{RESET}")
                sys.exit(1)
            location = {
                "lat": args.lat, "lon": args.lon,
                "name": f"{args.lat:.4f}", "country": "", "admin1": "",
            }

        # ── Fetch ─────────────────────────────────────────────────────────────
        print(f"  🌐 Fetching {args.days}-day forecast …")
        try:
            raw = fetch_forecast(client, location["lat"], location["lon"], args.days)
        except CircuitOpenError as exc:
            print(f"  {RED}❌  Circuit breaker open: {exc}{RESET}")
            sys.exit(1)
        except RateLimitError as exc:
            print(f"  {RED}❌  Rate limited: {exc}{RESET}")
            sys.exit(1)
        except APIError as exc:
            print(f"  {RED}❌  API error: {exc}{RESET}")
            if args.ai_debug:
                report = diagnose_error(
                    exc,
                    status_code=exc.status_code,
                    url=exc.url,
                    response_snippet=exc.response_body,
                )
                print(report)
            sys.exit(1)

        # ── Pipeline ──────────────────────────────────────────────────────────
        print(f"  ⚙️  Running data pipeline …")
        result = run_pipeline(
            raw_response=raw,
            location=location,
            db_path=args.db,
            t_start=t_start,
        )

        if not result.ok:
            print(f"  {RED}❌  Pipeline validation failed:{RESET}")
            for e in result.errors:
                print(f"       • {e}")
            if args.ai_debug:
                report = diagnose_error(
                    Exception("; ".join(result.errors)),
                    warnings=result.warnings,
                )
                print(report)
            sys.exit(1)

        # ── Display ───────────────────────────────────────────────────────────
        print_report(result)

        # ── Perf stats ────────────────────────────────────────────────────────
        perf = client.performance_report()
        if args.verbose:
            print(f"  {GREY}Perf: {json.dumps(perf)}{RESET}")

        # ── Export ────────────────────────────────────────────────────────────
        if args.export in ("json", "both"):
            export_json(result, f"{args.out}.json")
        if args.export in ("csv", "both"):
            export_csv(result, f"{args.out}.csv")

        # ── AI features ───────────────────────────────────────────────────────
        if args.ai_perf:
            print(f"\n  🤖 Requesting AI performance analysis …")
            try:
                cb_state = client.circuit_breaker.state.name
                report = analyse_performance(
                    perf_stats=perf,
                    days=args.days,
                    circuit_state=cb_state,
                    timeouts=(client.connect_timeout, client.read_timeout),
                )
                print(report)
            except Exception as exc:
                print(f"  {YELLOW}⚠  AI analysis unavailable: {exc}{RESET}")

        if args.ai_warn and result.warnings:
            print(f"\n  🤖 Requesting AI warning explanation …")
            try:
                report = explain_warnings(result.warnings, result.records)
                print(report)
            except Exception as exc:
                print(f"  {YELLOW}⚠  AI analysis unavailable: {exc}{RESET}")
        elif args.ai_warn and not result.warnings:
            print(f"  {GREEN}✅  No warnings to explain — pipeline ran cleanly.{RESET}\n")


if __name__ == "__main__":
    main()
