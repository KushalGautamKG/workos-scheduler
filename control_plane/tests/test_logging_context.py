"""Tests for Day 127 logging context enrichment."""

from __future__ import annotations

import logging

from control_plane.app.core.logging_context import (
    KernelQContextFilter,
    KernelQLoggerAdapter,
    attach_context_filter,
    current_trace_ids,
    load_logging_identity,
)


def test_load_logging_identity_defaults(monkeypatch) -> None:
    monkeypatch.delenv("KERNELQ_SERVICE_NAME", raising=False)
    monkeypatch.delenv("KERNELQ_ENVIRONMENT", raising=False)
    monkeypatch.delenv("KERNELQ_VERSION", raising=False)
    identity = load_logging_identity()
    assert identity["service"] == "kernelq-control-plane"
    assert identity["environment"] == "local"
    assert identity["version"] == "dev"


def test_context_filter_sets_base_fields(monkeypatch) -> None:
    monkeypatch.setenv("KERNELQ_ENVIRONMENT", "test")
    monkeypatch.setenv("KERNELQ_VERSION", "day127")
    filt = KernelQContextFilter(component="api", operation="enqueue")
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    assert filt.filter(record) is True
    assert record.service == "kernelq-control-plane"
    assert record.environment == "test"
    assert record.version == "day127"
    assert record.component == "api"
    assert record.operation == "enqueue"
    assert not hasattr(record, "trace_id") or getattr(record, "trace_id", None) in (None, "")


def test_current_trace_ids_without_otel() -> None:
    trace_id, span_id = current_trace_ids()
    assert trace_id is None
    assert span_id is None


def test_adapter_merges_identity(monkeypatch) -> None:
    monkeypatch.setenv("KERNELQ_ENVIRONMENT", "local")
    base = logging.getLogger("kernelq.logging_context.test_adapter")
    adapter = KernelQLoggerAdapter(base, {"component": "control_plane"})
    # Ensure process() injects fields without raising.
    msg, kwargs = adapter.process("ping", {})
    assert msg == "ping"
    assert kwargs["extra"]["service"] == "kernelq-control-plane"
    assert kwargs["extra"]["component"] == "control_plane"


def test_attach_context_filter_idempotent() -> None:
    logger = logging.getLogger("kernelq.logging_context.test_attach")
    logger.filters.clear()
    first = attach_context_filter(logger, component="cp")
    second = attach_context_filter(logger, component="other")
    assert first is second
    assert sum(1 for f in logger.filters if isinstance(f, KernelQContextFilter)) == 1
