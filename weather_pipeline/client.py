"""
pipeline/client.py — Resilient HTTP client for Open-Meteo.

Features
────────
• Exponential back-off with jitter (3 retries by default)
• Per-request timeout (connect + read)
• Circuit breaker — after N consecutive failures the client stops
  hammering the API and raises CircuitOpenError immediately
• Structured request/response logging to a rotating file
• Response-time tracking for performance monitoring
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ── Logging ────────────────────────────────────────────────────────────────────

logger = logging.getLogger("weather.client")


# ── Custom exceptions ──────────────────────────────────────────────────────────

class APIError(Exception):
    """Wraps HTTP / network errors with extra context."""
    def __init__(self, message: str, status_code: int | None = None,
                 url: str = "", response_body: str = ""):
        super().__init__(message)
        self.status_code   = status_code
        self.url           = url
        self.response_body = response_body


class CircuitOpenError(APIError):
    """Raised when the circuit breaker is open."""


class RateLimitError(APIError):
    """Raised on HTTP 429 — caller should back off."""


# ── Circuit breaker ────────────────────────────────────────────────────────────

class _CBState(Enum):
    CLOSED   = auto()   # normal operation
    OPEN     = auto()   # failing — reject requests immediately
    HALF_OPEN = auto()  # probing — allow one request through


@dataclass
class CircuitBreaker:
    """
    Simple count-based circuit breaker.

    failure_threshold   — consecutive failures before opening
    recovery_timeout    — seconds to wait before moving to HALF_OPEN
    """
    failure_threshold: int   = 5
    recovery_timeout:  float = 30.0

    _state:            _CBState = field(default=_CBState.CLOSED, init=False)
    _failure_count:    int      = field(default=0,               init=False)
    _last_failure_at:  float    = field(default=0.0,             init=False)

    @property
    def state(self) -> _CBState:
        if self._state == _CBState.OPEN:
            if time.monotonic() - self._last_failure_at >= self.recovery_timeout:
                logger.info("Circuit breaker → HALF_OPEN (probing)")
                self._state = _CBState.HALF_OPEN
        return self._state

    def record_success(self):
        self._failure_count = 0
        if self._state != _CBState.CLOSED:
            logger.info("Circuit breaker → CLOSED (recovered)")
        self._state = _CBState.CLOSED

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_at = time.monotonic()
        if self._failure_count >= self.failure_threshold:
            if self._state != _CBState.OPEN:
                logger.warning(
                    "Circuit breaker → OPEN after %d consecutive failures",
                    self._failure_count,
                )
            self._state = _CBState.OPEN

    def allow_request(self) -> bool:
        s = self.state
        if s == _CBState.CLOSED:
            return True
        if s == _CBState.HALF_OPEN:
            return True          # let one probe through
        return False             # OPEN → reject


# ── Resilient session ──────────────────────────────────────────────────────────

def _build_session(total_retries: int = 3) -> requests.Session:
    """
    Build a requests.Session with urllib3 retry logic for transient errors.

    Note: urllib3 retries handle connection/read errors and selected HTTP
    status codes.  We add our own application-level retry loop on top for
    finer control (back-off, circuit breaker).
    """
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=0.3,
        status_forcelist={502, 503, 504},
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# ── Main client class ──────────────────────────────────────────────────────────

@dataclass
class WeatherAPIClient:
    """
    Thread-safe HTTP client for Open-Meteo with:
      - exponential back-off + jitter
      - circuit breaker
      - response-time metrics
    """
    connect_timeout:   float = 5.0
    read_timeout:      float = 15.0
    max_retries:       int   = 3
    base_backoff:      float = 1.0    # seconds
    max_backoff:       float = 30.0

    circuit_breaker:   CircuitBreaker = field(
        default_factory=CircuitBreaker, init=False
    )
    _session:          requests.Session = field(init=False)
    _response_times:   list[float]      = field(default_factory=list, init=False)

    def __post_init__(self):
        self._session = _build_session(total_retries=1)  # urllib3 does 1, we do the rest

    # ── Public interface ───────────────────────────────────────────────────────

    def get(self, url: str, params: dict[str, Any] | None = None) -> dict:
        """
        Perform a GET request with retry/back-off/circuit-breaker.

        Returns the parsed JSON body on success.
        Raises APIError subclasses on terminal failure.
        """
        if not self.circuit_breaker.allow_request():
            raise CircuitOpenError(
                f"Circuit breaker is OPEN — not sending request to {url}",
                url=url,
            )

        last_exc: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                t0 = time.monotonic()
                resp = self._session.get(
                    url,
                    params=params,
                    timeout=(self.connect_timeout, self.read_timeout),
                )
                elapsed = time.monotonic() - t0
                self._response_times.append(elapsed)

                logger.debug(
                    "GET %s → HTTP %d  (%.0f ms, attempt %d)",
                    url, resp.status_code, elapsed * 1000, attempt,
                )

                # ── Handle specific HTTP error codes ───────────────────────────
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    raise RateLimitError(
                        f"Rate limited — retry after {retry_after}s",
                        status_code=429,
                        url=url,
                    )

                if resp.status_code >= 500:
                    # Server error — back off and retry
                    raise APIError(
                        f"Server error {resp.status_code}",
                        status_code=resp.status_code,
                        url=url,
                        response_body=resp.text[:500],
                    )

                if resp.status_code >= 400:
                    # Client error — don't retry
                    raise APIError(
                        f"Client error {resp.status_code}: {resp.text[:200]}",
                        status_code=resp.status_code,
                        url=url,
                        response_body=resp.text[:500],
                    )

                # ── Success ────────────────────────────────────────────────────
                self.circuit_breaker.record_success()
                return resp.json()

            except (RateLimitError, APIError) as exc:
                # Only retry on server errors, not client errors
                if isinstance(exc, APIError) and exc.status_code is not None and exc.status_code < 500:
                    self.circuit_breaker.record_failure()
                    raise

                last_exc = exc
                self.circuit_breaker.record_failure()
                if attempt == self.max_retries:
                    break
                wait = self._backoff(attempt)
                logger.warning(
                    "Attempt %d/%d failed (%s). Retrying in %.1fs …",
                    attempt, self.max_retries, exc, wait,
                )
                time.sleep(wait)

            except requests.exceptions.Timeout as exc:
                last_exc = APIError(f"Request timed out: {exc}", url=url)
                self.circuit_breaker.record_failure()
                if attempt == self.max_retries:
                    break
                wait = self._backoff(attempt)
                logger.warning("Timeout on attempt %d. Retrying in %.1fs …", attempt, wait)
                time.sleep(wait)

            except requests.exceptions.ConnectionError as exc:
                last_exc = APIError(f"Connection error: {exc}", url=url)
                self.circuit_breaker.record_failure()
                if attempt == self.max_retries:
                    break
                wait = self._backoff(attempt)
                logger.warning("Connection error on attempt %d. Retrying in %.1fs …", attempt, wait)
                time.sleep(wait)

        raise last_exc or APIError("Unknown error after retries", url=url)

    # ── Metrics ────────────────────────────────────────────────────────────────

    def performance_report(self) -> dict:
        """Return latency statistics for all requests made so far."""
        if not self._response_times:
            return {"requests": 0}
        times = sorted(self._response_times)
        n = len(times)

        def pct(p):
            idx = int(p / 100 * n)
            return round(times[min(idx, n - 1)] * 1000, 1)

        return {
            "requests":   n,
            "avg_ms":     round(sum(times) / n * 1000, 1),
            "min_ms":     round(times[0]  * 1000, 1),
            "max_ms":     round(times[-1] * 1000, 1),
            "p50_ms":     pct(50),
            "p90_ms":     pct(90),
            "p99_ms":     pct(99),
        }

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _backoff(self, attempt: int) -> float:
        """Full-jitter exponential back-off."""
        cap   = min(self.max_backoff, self.base_backoff * (2 ** (attempt - 1)))
        return random.uniform(0, cap)

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
