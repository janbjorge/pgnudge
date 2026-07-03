"""Feed contract and coalescing engine, pure stdlib.

A feed emits ``Resync`` (reload everything) and ``Batch`` (coalesced
wakeups) — at-least-once, every gap bracketed by a Resync. See README.
"""

import asyncio
import contextlib
import random
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Self

__all__ = ["Event", "Batch", "Resync", "FeedItem", "BaseFeed"]


@dataclass(frozen=True, slots=True)
class Event:
    """One coalesced wakeup; ``count`` = arrivals of this (channel, payload) in the window."""

    channel: str
    payload: str
    first_seen: float  # time.time() of first arrival in this batch
    count: int = 1


@dataclass(frozen=True, slots=True)
class Batch:
    """A debounce window's worth of events, deduplicated, in arrival order."""

    events: tuple[Event, ...]

    def payloads(self) -> tuple[str, ...]:
        return tuple(e.payload for e in self.events)


@dataclass(frozen=True, slots=True)
class Resync:
    """Reload-everything signal; reason is "connected" | "reconnected" | "overflow" | "failsafe"."""

    reason: str


type FeedItem = Resync | Batch


class BaseFeed:
    """Queue/debounce/backoff engine behind the async-iterator surface.

    Subclasses implement ``_supervisor`` (call ``_emit_resync`` per
    (re)connect, ``_push_raw`` per wakeup) and may override ``_extra_close``.
    """

    def __init__(
        self,
        *,
        debounce: float = 0.05,
        max_batch_wait: float | None = None,
        failsafe: float | None = None,
        backoff: tuple[float, float] = (0.1, 5.0),
        raw_queue_size: int = 8192,
        payload_filter: Callable[[str], bool] | None = None,
    ) -> None:
        self._debounce = debounce
        self._max_batch_wait = max_batch_wait if max_batch_wait is not None else debounce * 20
        self._failsafe = failsafe
        self._backoff_initial, self._backoff_max = backoff
        self._payload_filter = payload_filter

        self._raw: asyncio.Queue[tuple[str, str, float]] = asyncio.Queue(maxsize=raw_queue_size)
        self._out: asyncio.Queue[FeedItem | None] = asyncio.Queue()  # None = closed
        self._overflowed = False

        self._tasks: list[asyncio.Task[None]] = []
        self._started = False
        self._closing = False
        self.connection_pid: int | None = None  # server backend pid while connected

    # -- lifecycle -----------------------------------------------------------

    async def __aenter__(self) -> Self:
        self._ensure_started()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def _ensure_started(self) -> None:
        if self._started:
            return
        self._started = True
        self._tasks.append(asyncio.create_task(self._supervisor(), name=f"{type(self).__name__}-supervisor"))
        self._tasks.append(asyncio.create_task(self._debouncer(), name=f"{type(self).__name__}-debouncer"))
        if self._failsafe is not None:
            self._tasks.append(asyncio.create_task(self._failsafe_loop(), name=f"{type(self).__name__}-failsafe"))

    async def aclose(self) -> None:
        if self._closing:
            return
        self._closing = True
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        await self._extra_close()
        self.connection_pid = None
        self._out.put_nowait(None)

    async def _extra_close(self) -> None:  # pragma: no cover - subclass hook
        return

    # -- consumer side -------------------------------------------------------

    def __aiter__(self) -> AsyncIterator[FeedItem]:
        self._ensure_started()
        return self

    async def __anext__(self) -> FeedItem:
        item = await self._out.get()
        if item is None:
            raise StopAsyncIteration
        return item

    # -- transport-facing helpers ---------------------------------------------

    def _push_raw(self, channel: str, payload: str) -> None:
        if self._payload_filter is not None and not self._payload_filter(payload):
            return
        try:
            self._raw.put_nowait((channel, payload, time.time()))
        except asyncio.QueueFull:
            self._overflowed = True

    def _emit_resync(self, reason: str) -> None:
        self._out.put_nowait(Resync(reason))

    def _backoff_delay(self, attempt: int) -> float:
        base = min(self._backoff_max, self._backoff_initial * (2.0 ** min(attempt - 1, 16)))
        return base * random.uniform(0.5, 1.5)

    async def _supervisor(self) -> None:  # pragma: no cover - abstract
        raise NotImplementedError

    # -- coalescing ----------------------------------------------------------

    def _consume_overflow(self) -> bool:
        """Check-and-clear the overflow flag (set concurrently by _push_raw)."""
        if self._overflowed:
            self._overflowed = False
            self._drain_raw()
            return True
        return False

    async def _debouncer(self) -> None:
        while True:
            channel, payload, at = await self._raw.get()

            if self._consume_overflow():
                self._emit_resync("overflow")
                continue

            buf: dict[tuple[str, str], Event] = {}
            self._absorb(buf, channel, payload, at)
            hard_deadline = time.monotonic() + self._max_batch_wait
            while True:
                remaining = min(self._debounce, hard_deadline - time.monotonic())
                if remaining <= 0:
                    break
                try:
                    channel, payload, at = await asyncio.wait_for(self._raw.get(), remaining)
                except (asyncio.TimeoutError, TimeoutError):
                    break
                self._absorb(buf, channel, payload, at)

            if self._consume_overflow():
                self._emit_resync("overflow")
            else:
                self._out.put_nowait(Batch(tuple(buf.values())))

    @staticmethod
    def _absorb(buf: dict[tuple[str, str], Event], channel: str, payload: str, at: float) -> None:
        key = (channel, payload)
        prev = buf.get(key)
        if prev is None:
            buf[key] = Event(channel=channel, payload=payload, first_seen=at)
        else:
            buf[key] = Event(channel=channel, payload=payload, first_seen=prev.first_seen, count=prev.count + 1)

    def _drain_raw(self) -> None:
        while True:
            try:
                self._raw.get_nowait()
            except asyncio.QueueEmpty:
                return

    async def _failsafe_loop(self) -> None:
        assert self._failsafe is not None
        while True:
            await asyncio.sleep(self._failsafe)
            self._emit_resync("failsafe")
