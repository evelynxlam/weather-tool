"""
pipeline/ai_debugger.py — AI-powered integration debugger & optimiser.

Uses the Anthropic API to:
  1. Diagnose API integration issues from error context
  2. Analyse pipeline warnings and suggest fixes
  3. Review performance metrics and recommend optimisations
  4. Explain validation failures in plain English

The debugger is invoked explicitly (never runs automatically) so it only
incurs API cost when the user requests it.
"""

from __future__ import annotations

import json
import logging
import os
import textwrap
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("weather.ai_debugger")

# ── Optional Anthropic dependency ─────────────────────────────────────────────
# We import lazily so the rest of the tool works without the SDK installed.

def _anthropic():
    try:
        import anthropic
        return anthropic
    except ImportError:
        raise ImportError(
            "anthropic SDK not installed. Run: pip install anthropic"
        )


# ── Prompt templates ──────────────────────────────────────────────────────────

_SYSTEM = textwrap.dedent("""\
    You are an expert Python backend engineer specialising in REST API
    integration, data pipeline reliability, and performance optimisation.
    You give concise, actionable advice with concrete code examples.
    Always structure your response with:
      • Root Cause  — what went wrong and why
      • Fix         — specific code change or configuration tweak
      • Prevention  — how to avoid recurrence
    Keep each section to 3–5 bullet points. Use plain English.
""")

_DIAGNOSE_PROMPT = """\
A weather data pipeline encountered the following issue.

=== ERROR CONTEXT ===
{error_context}

=== RECENT WARNINGS ===
{warnings}

=== HTTP DETAILS ===
Status code : {status_code}
URL         : {url}
Response    : {response_snippet}

Diagnose the root cause and provide a fix.
"""

_PERF_PROMPT = """\
Analyse these API performance metrics for a weather data pipeline and
recommend optimisations.

=== LATENCY STATS (ms) ===
{perf_stats}

=== REQUEST PATTERN ===
{request_pattern}

=== PIPELINE CONFIG ===
{pipeline_config}

Focus on: caching, connection pooling, batching, and timeout tuning.
"""

_WARN_PROMPT = """\
A data validation pipeline produced the following warnings when processing
an Open-Meteo API response.  Explain what each warning means, whether
the processed data can be trusted, and what code changes would prevent them.

=== VALIDATION WARNINGS ===
{warnings}

=== SAMPLE RECORDS (first 3) ===
{sample_records}
"""


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class DebugReport:
    mode:       str
    prompt:     str
    response:   str
    model:      str
    input_tokens:  int = 0
    output_tokens: int = 0

    def __str__(self):
        sep = "─" * 60
        return (
            f"\n{sep}\n"
            f"  🤖  AI Debug Report — {self.mode}\n"
            f"{sep}\n"
            f"{self.response}\n"
            f"{sep}\n"
            f"  Model: {self.model}  |  "
            f"Tokens in: {self.input_tokens}  out: {self.output_tokens}\n"
        )


# ── Core helper ───────────────────────────────────────────────────────────────

def _call_claude(prompt: str, model: str = "claude-sonnet-4-20250514") -> DebugReport:
    sdk = _anthropic()
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY environment variable is not set."
        )
    client = sdk.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=model,
        max_tokens=1024,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text, msg.usage.input_tokens, msg.usage.output_tokens


# ── Public debugger functions ─────────────────────────────────────────────────

def diagnose_error(
    error: Exception,
    warnings: list[str] | None = None,
    status_code: int | None = None,
    url: str = "",
    response_snippet: str = "",
) -> DebugReport:
    """
    Ask Claude to diagnose an API or pipeline error.

    Parameters
    ----------
    error            : the caught exception
    warnings         : validation warnings from the same run
    status_code      : HTTP status code if available
    url              : the request URL
    response_snippet : first ~500 chars of the response body
    """
    prompt = _DIAGNOSE_PROMPT.format(
        error_context=f"{type(error).__name__}: {error}",
        warnings="\n".join(warnings or ["(none)"]),
        status_code=status_code or "N/A",
        url=url or "N/A",
        response_snippet=response_snippet or "(none)",
    )
    text, tin, tout = _call_claude(prompt)
    return DebugReport(
        mode="Error Diagnosis",
        prompt=prompt,
        response=text,
        model="claude-sonnet-4-20250514",
        input_tokens=tin,
        output_tokens=tout,
    )


def analyse_performance(
    perf_stats: dict,
    days: int = 7,
    retries_used: int = 0,
    circuit_state: str = "CLOSED",
    timeouts: tuple[float, float] = (5.0, 15.0),
) -> DebugReport:
    """
    Ask Claude to review latency metrics and recommend optimisations.
    """
    request_pattern = (
        f"Forecast days requested : {days}\n"
        f"Retries used            : {retries_used}\n"
        f"Circuit breaker state   : {circuit_state}"
    )
    pipeline_config = (
        f"Connect timeout : {timeouts[0]}s\n"
        f"Read timeout    : {timeouts[1]}s\n"
        f"Max retries     : 3\n"
        f"Back-off        : exponential with full jitter"
    )
    prompt = _PERF_PROMPT.format(
        perf_stats=json.dumps(perf_stats, indent=2),
        request_pattern=request_pattern,
        pipeline_config=pipeline_config,
    )
    text, tin, tout = _call_claude(prompt)
    return DebugReport(
        mode="Performance Analysis",
        prompt=prompt,
        response=text,
        model="claude-sonnet-4-20250514",
        input_tokens=tin,
        output_tokens=tout,
    )


def explain_warnings(
    warnings: list[str],
    sample_records: list[dict] | None = None,
) -> DebugReport:
    """
    Ask Claude to explain validation warnings and recommend fixes.
    """
    sample = json.dumps((sample_records or [])[:3], indent=2)
    prompt = _WARN_PROMPT.format(
        warnings="\n".join(warnings),
        sample_records=sample,
    )
    text, tin, tout = _call_claude(prompt)
    return DebugReport(
        mode="Warning Explanation",
        prompt=prompt,
        response=text,
        model="claude-sonnet-4-20250514",
        input_tokens=tin,
        output_tokens=tout,
    )
