"""
Idempotency store boundary for duplicate suppression.

KernelQ will use this interface at Kafka handoffs (dispatch intake, worker
execution, result consumption). Callers ask: "May I process this logical
event for the first time?" — not "What is the job's durable state?" (that
stays in Postgres).

``try_claim`` mirrors Redis ``SET key value NX EX ttl`` in spirit:
- ``True``  → first claimant; proceed with side effects
- ``False`` → duplicate while the key is still live; skip or no-op

This module uses only the Python standard library. A Redis-backed
implementation comes in a later milestone (Day 99).
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Callable


def _validate_claim_inputs(key: str, ttl_seconds: int) -> None:
    """
    Reject invalid arguments before touching any store.

    Every implementation should call this (or equivalent checks) at the start
    of ``try_claim``.
    """
    if not key or not key.strip():
        raise ValueError("key must be non-empty")
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be a positive integer")


class IdempotencyStore(ABC):
    """
    Abstract duplicate-suppression store.

    Subclasses implement ``try_claim`` for a specific backend (in-memory,
    Redis, etc.). Handlers depend on this type — not on Redis directly —
    so tests can inject ``InMemoryIdempotencyStore`` without a broker.
    """

    @abstractmethod
    def try_claim(self, key: str, ttl_seconds: int) -> bool:
        """
        Attempt to claim an idempotency key for ``ttl_seconds``.

        Returns:
            ``True`` if this caller is the first to claim the key (or the
            previous claim expired). Safe to process the event.

            ``False`` if another caller already holds a non-expired claim.
            Treat as a duplicate and skip side effects.
        """
        raise NotImplementedError


class InMemoryIdempotencyStore(IdempotencyStore):
    """
    In-memory idempotency store for local tests and development.

    Each key maps to an **expiry timestamp** (seconds since epoch, from an
    injectable clock). When ``try_claim`` succeeds, the key lives until
    ``now + ttl_seconds``.

    Not shared across processes — use a Redis-backed store in production.
    """

    def __init__(self, *, now: Callable[[], float] | None = None) -> None:
        # Default to real wall clock; tests pass a fixed ``now`` lambda.
        self._now: Callable[[], float] = now if now is not None else time.time
        # key -> expiry time (float seconds)
        self._expiry_by_key: dict[str, float] = {}

    def try_claim(self, key: str, ttl_seconds: int) -> bool:
        _validate_claim_inputs(key, ttl_seconds)

        current_time = self._now()
        expiry = self._expiry_by_key.get(key)

        # Missing key, or previous TTL elapsed → new claim.
        if expiry is None or expiry <= current_time:
            self._expiry_by_key[key] = current_time + ttl_seconds
            return True

        # Key still live → duplicate.
        return False

    def cleanup_expired(self) -> int:
        """
        Remove all keys whose TTL has passed.

        Returns the number of keys removed. Useful in long-running tests
        to mimic Redis automatic expiry without waiting in real time.
        """
        current_time = self._now()
        expired_keys = [
            stored_key
            for stored_key, expiry in self._expiry_by_key.items()
            if expiry <= current_time
        ]
        for stored_key in expired_keys:
            del self._expiry_by_key[stored_key]
        return len(expired_keys)
