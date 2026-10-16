# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Bounded flush and shutdown coordination for independent telemetry providers."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

_PROVIDER_NAME = re.compile(r"[a-z0-9][a-z0-9_.-]{0,63}\Z")
_MAX_PROVIDERS = 32
_MAX_TIMEOUT_SECONDS = 300.0


class TelemetrySignal(StrEnum):
    """Telemetry signal a provider owns and can flush during application shutdown."""

    LOGS = "logs"
    METRICS = "metrics"
    TRACES = "traces"


@runtime_checkable
class TelemetryProvider(Protocol):
    """Lifecycle contract for one independently configured telemetry provider/exporter."""

    @property
    def name(self) -> str:
        """Return a stable, non-secret slug used to identify safe lifecycle failures."""

    @property
    def signals(self) -> frozenset[TelemetrySignal]:
        """Declare which observability signals this provider exports."""

    async def force_flush(self, *, timeout_seconds: float) -> None:
        """Export buffered telemetry and honor the supplied deadline.

        Implementations should pass the deadline to the underlying SDK and propagate task
        cancellation; Python cannot forcibly stop a callback that suppresses cancellation.
        """

    async def shutdown(self, *, timeout_seconds: float) -> None:
        """Release owned resources idempotently and honor the supplied deadline.

        Implementations should pass the deadline to the underlying SDK and propagate task
        cancellation so application shutdown can finish within its budget.
        """


class LifecycleOperation(StrEnum):
    """Lifecycle action reported when a provider fails or exceeds its deadline."""

    FLUSH = "flush"
    SHUTDOWN = "shutdown"


@dataclass(frozen=True, slots=True)
class LifecycleFailure:
    """Sanitized lifecycle failure without the vendor exception or telemetry payload."""

    provider: str
    operation: LifecycleOperation
    code: str

    def __post_init__(self) -> None:
        """Keep even manually constructed failure records safe for logs and diagnostics."""
        if not isinstance(self.provider, str) or _PROVIDER_NAME.fullmatch(self.provider) is None:
            raise ValueError("Lifecycle failure provider must be a lowercase non-secret slug.")
        if not isinstance(self.operation, LifecycleOperation):
            raise TypeError("Lifecycle failure operation must be a LifecycleOperation.")
        if self.code not in {"timeout", "provider_failure"}:
            raise ValueError("Lifecycle failure code must be a stable category.")


class ObservabilityLifecycleError(RuntimeError):
    """Raised after all eligible providers were attempted and at least one failed safely."""

    def __init__(self, failures: Sequence[LifecycleFailure]) -> None:
        """Capture bounded failure identifiers and omit all original provider exceptions."""
        if not failures or len(failures) > _MAX_PROVIDERS:
            raise ValueError("Lifecycle errors require 1 through 32 provider failures.")
        self.failures = tuple(failures)
        summary = ", ".join(
            f"{failure.provider}:{failure.operation.value}:{failure.code}"
            for failure in self.failures
        )
        super().__init__(f"Observability lifecycle failed for {summary}.")


class ObservabilityLifecycle:
    """Coordinate flush and shutdown across explicitly selected telemetry providers.

    Providers are not discovered, configured or started implicitly. The application configures
    each optional signal adapter and transfers its lifecycle ownership to this coordinator.
    Operations run concurrently up to ``max_concurrency`` and receive a per-provider deadline.
    Provider tasks are cancelled at their deadline, and a callback that suppresses cancellation is
    still reported as timed out when it completes. Python cannot forcibly terminate such a callback,
    so providers must also configure native SDK deadlines and propagate cancellation. Shutdown
    flushes before releasing providers and remains shielded from waiter cancellation. Later calls
    retry incomplete provider shutdown stages without repeating completed shutdowns.
    """

    def __init__(
        self,
        providers: Sequence[TelemetryProvider],
        *,
        max_concurrency: int = 8,
    ) -> None:
        """Validate a detached, bounded provider registry and explicit concurrency cap."""
        if isinstance(providers, (str, bytes)) or not isinstance(providers, Sequence):
            raise TypeError("providers must be a sequence of telemetry providers.")
        if not 1 <= len(providers) <= _MAX_PROVIDERS:
            raise ValueError("Observability lifecycle requires 1 through 32 providers.")
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise TypeError("max_concurrency must be an integer.")
        if not 1 <= max_concurrency <= _MAX_PROVIDERS:
            raise ValueError("max_concurrency must be from 1 through 32.")
        detached = tuple(providers)
        named_providers: list[tuple[str, TelemetryProvider]] = []
        names: list[str] = []
        for provider in detached:
            if not isinstance(provider, TelemetryProvider):
                raise TypeError("Every provider must implement TelemetryProvider.")
            name = provider.name
            signals = provider.signals
            if not isinstance(name, str) or _PROVIDER_NAME.fullmatch(name) is None:
                raise ValueError("Provider names must be unique lowercase non-secret slugs.")
            if (
                not isinstance(signals, frozenset)
                or not signals
                or any(not isinstance(signal, TelemetrySignal) for signal in signals)
            ):
                raise ValueError("Providers must declare one or more known telemetry signals.")
            names.append(name)
            named_providers.append((name, provider))
        if len(names) != len(set(names)):
            raise ValueError("Telemetry provider names must be unique.")
        self._providers = tuple(named_providers)
        self._max_concurrency = min(max_concurrency, len(detached))
        self._lock = asyncio.Lock()
        self._closing = False
        self._shutdown_task: asyncio.Task[tuple[LifecycleFailure, ...]] | None = None
        self._flushed: set[str] = set()
        self._stopped: set[str] = set()

    @property
    def closing(self) -> bool:
        """Report whether shutdown has begun."""
        return self._closing

    async def force_flush(self, *, timeout_seconds: float = 10.0) -> None:
        """Flush all providers and raise one sanitized aggregate after attempting every provider."""
        timeout = _validate_timeout(timeout_seconds)
        if self._closing:
            raise RuntimeError("Observability providers are shutting down.")
        async with self._lock:
            if self._closing:
                raise RuntimeError("Observability providers are shutting down.")
            failures = await self._run_all(LifecycleOperation.FLUSH, timeout)
        if failures:
            raise ObservabilityLifecycleError(failures) from None

    async def shutdown(self, *, timeout_seconds: float = 10.0) -> None:
        """Flush and release providers, retrying incomplete cleanup across later calls.

        Concurrent callers share an in-flight attempt. A cancelled waiter does not cancel that
        attempt. Later calls retry failed flush or shutdown stages, while successful stages are
        not repeated; providers should still implement idempotent shutdown as required by the
        :class:`TelemetryProvider` contract.
        """
        timeout = _validate_timeout(timeout_seconds)
        task = self._shutdown_task
        if task is not None and task.done() and len(self._stopped) < len(self._providers):
            task = None
        if task is None:
            self._closing = True
            task = asyncio.create_task(self._shutdown_providers(timeout))
            self._shutdown_task = task
        failures = await asyncio.shield(task)
        if failures:
            raise ObservabilityLifecycleError(failures) from None

    async def _shutdown_providers(self, timeout_seconds: float) -> tuple[LifecycleFailure, ...]:
        """Flush pending providers, then release each provider even when flushing failed."""
        async with self._lock:
            failures: list[LifecycleFailure] = []
            pending_flush = tuple(
                (name, provider)
                for name, provider in self._providers
                if name not in self._flushed and name not in self._stopped
            )
            flush_failures = await self._run_all(
                LifecycleOperation.FLUSH, timeout_seconds, providers=pending_flush
            )
            failures.extend(flush_failures)
            failed_flush_names = {failure.provider for failure in flush_failures}
            self._flushed.update(
                name for name, _ in pending_flush if name not in failed_flush_names
            )

            pending_shutdown = tuple(
                (name, provider) for name, provider in self._providers if name not in self._stopped
            )
            shutdown_failures = await self._run_all(
                LifecycleOperation.SHUTDOWN, timeout_seconds, providers=pending_shutdown
            )
            failures.extend(shutdown_failures)
            failed_shutdown_names = {failure.provider for failure in shutdown_failures}
            self._stopped.update(
                name for name, _ in pending_shutdown if name not in failed_shutdown_names
            )
            return tuple(failures)

    async def _run_all(
        self,
        operation: LifecycleOperation,
        timeout_seconds: float,
        *,
        providers: Sequence[tuple[str, TelemetryProvider]] | None = None,
    ) -> tuple[LifecycleFailure, ...]:
        """Run one operation with bounded concurrency and retain only safe error identifiers."""
        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def run(name: str, provider: TelemetryProvider) -> LifecycleFailure | None:
            async with semaphore:
                started = asyncio.get_running_loop().time()
                try:
                    async with asyncio.timeout(timeout_seconds):
                        if operation is LifecycleOperation.FLUSH:
                            await provider.force_flush(timeout_seconds=timeout_seconds)
                        else:
                            await provider.shutdown(timeout_seconds=timeout_seconds)
                except TimeoutError:
                    return LifecycleFailure(name, operation, "timeout")
                except Exception:
                    return LifecycleFailure(name, operation, "provider_failure")
                if asyncio.get_running_loop().time() - started >= timeout_seconds:
                    return LifecycleFailure(name, operation, "timeout")
                return None

        selected = self._providers if providers is None else providers
        if not selected:
            return ()
        results = await asyncio.gather(*(run(name, provider) for name, provider in selected))
        return tuple(result for result in results if result is not None)


def _validate_timeout(value: float) -> float:
    """Require a finite positive per-provider deadline no greater than five minutes."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("timeout_seconds must be a number of seconds.")
    if isinstance(value, int):
        if not 0 < value <= _MAX_TIMEOUT_SECONDS:
            raise ValueError("timeout_seconds must be greater than zero and at most 300 seconds.")
        return float(value)
    timeout = float(value)
    if not math.isfinite(timeout) or not 0 < timeout <= _MAX_TIMEOUT_SECONDS:
        raise ValueError("timeout_seconds must be greater than zero and at most 300 seconds.")
    return timeout


__all__ = [
    "LifecycleFailure",
    "LifecycleOperation",
    "ObservabilityLifecycle",
    "ObservabilityLifecycleError",
    "TelemetryProvider",
    "TelemetrySignal",
]
