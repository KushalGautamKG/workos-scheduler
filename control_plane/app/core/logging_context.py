"""
Logging context enrichment for KernelQ control plane (Day 127).

Attaches shared contract fields (service, environment, version) and optional
OpenTelemetry trace_id / span_id when a span context is available.
Extends stdlib logging; does not replace format_log_event key=value helpers.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

DEFAULT_SERVICE_NAME = "kernelq-control-plane"
DEFAULT_ENVIRONMENT = "local"
DEFAULT_VERSION = "dev"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "json"


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None:
        return default
    trimmed = value.strip()
    return trimmed if trimmed else default


def load_logging_identity() -> dict[str, str]:
    """Load KERNELQ_* identity fields aligned with the Go worker where possible."""
    return {
        "service": _env("KERNELQ_SERVICE_NAME", DEFAULT_SERVICE_NAME),
        "environment": _env("KERNELQ_ENVIRONMENT", DEFAULT_ENVIRONMENT),
        "version": _env("KERNELQ_VERSION", DEFAULT_VERSION),
        "log_level": _env("KERNELQ_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
        "log_format": _env("KERNELQ_LOG_FORMAT", DEFAULT_LOG_FORMAT).lower(),
    }


def current_trace_ids() -> tuple[Optional[str], Optional[str]]:
    """
    Return (trace_id, span_id) as lowercase hex when an OTel span is valid.

    OpenTelemetry is optional — if the package or span is unavailable, both
    identifiers are omitted (returned as None).
    """
    try:
        from opentelemetry import trace  # type: ignore
    except ImportError:
        return None, None

    span = trace.get_current_span()
    if span is None:
        return None, None
    ctx = span.get_span_context()
    if ctx is None or not getattr(ctx, "is_valid", False):
        return None, None
    try:
        trace_id = format(ctx.trace_id, "032x")
        span_id = format(ctx.span_id, "016x")
    except (AttributeError, TypeError, ValueError):
        return None, None
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None, None
    return trace_id, span_id


class KernelQContextFilter(logging.Filter):
    """
    Filter that injects KernelQ base + correlation fields onto LogRecord.

    Contextual fields are only set when available (trace/span omitted when invalid).
    """

    def __init__(
        self,
        *,
        service: Optional[str] = None,
        environment: Optional[str] = None,
        version: Optional[str] = None,
        component: Optional[str] = None,
        operation: Optional[str] = None,
    ) -> None:
        super().__init__()
        identity = load_logging_identity()
        self.service = service or identity["service"]
        self.environment = environment or identity["environment"]
        self.version = version or identity["version"]
        self.component = component
        self.operation = operation

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self.service  # type: ignore[attr-defined]
        record.environment = self.environment  # type: ignore[attr-defined]
        record.version = self.version  # type: ignore[attr-defined]
        if self.component is not None:
            record.component = self.component  # type: ignore[attr-defined]
        if self.operation is not None:
            record.operation = self.operation  # type: ignore[attr-defined]

        trace_id, span_id = current_trace_ids()
        if trace_id is not None:
            record.trace_id = trace_id  # type: ignore[attr-defined]
        if span_id is not None:
            record.span_id = span_id  # type: ignore[attr-defined]
        return True


class KernelQLoggerAdapter(logging.LoggerAdapter):
    """Adapter that merges process identity and optional operation fields."""

    def process(self, msg: str, kwargs: Any) -> tuple[str, Any]:
        extra = dict(kwargs.get("extra") or {})
        identity = load_logging_identity()
        extra.setdefault("service", identity["service"])
        extra.setdefault("environment", identity["environment"])
        extra.setdefault("version", identity["version"])
        if self.extra:
            for key, value in self.extra.items():
                extra.setdefault(key, value)
        trace_id, span_id = current_trace_ids()
        if trace_id is not None:
            extra.setdefault("trace_id", trace_id)
        if span_id is not None:
            extra.setdefault("span_id", span_id)
        kwargs["extra"] = extra
        return msg, kwargs


def attach_context_filter(
    logger: Optional[logging.Logger] = None,
    *,
    component: Optional[str] = None,
    operation: Optional[str] = None,
) -> KernelQContextFilter:
    """Attach KernelQContextFilter to a logger (root by default). Idempotent by name."""
    target = logger or logging.getLogger()
    filt = KernelQContextFilter(component=component, operation=operation)
    # Avoid stacking identical filters on repeated startup.
    for existing in target.filters:
        if isinstance(existing, KernelQContextFilter):
            return existing
    target.addFilter(filt)
    return filt
