"""The machinery behind a feed, one class per concern, pure stdlib.

``Intake`` buffers raw wakeups, ``Coalescer`` dedups them, ``Debouncer``
decides when a window closes, ``Backoff`` paces reconnects, and
``FeedService`` wires them together behind the async-iterator surface
that ``BaseFeed`` exposes.
"""

import asyncio
import contextlib
import random
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self

from pgwake.core import Batch, Event, FeedItem, Resync

__all__ = ["Wakeup", "Intake", "Coalescer", "Debouncer", "Backoff", "FeedService", "BaseFeed"]


@dataclass(frozen=True, slots=True)
class Wakeup:
    """One raw arrival from a transport, pre-coalescing."""

    payload: str
    at: float  # time.time() of arrival


@dataclass(slots=True, kw_only=True)
class Intake:
    """Bounded wakeup buffer; overflow is flagged, never blocks the producer."""

    maxsize: int
    queue: asyncio.Queue[Wakeup] = field(init=False, repr=False)
    overflowed: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.maxsize)

    def push(self, payload: str) -> None:
        try:
            self.queue.put_nowait(Wakeup(payload=payload, at=time.time()))
        except asyncio.QueueFull:
            self.overflowed = True

    async def get(self) -> Wakeup:
        return await self.queue.get()

    async def get_within(self, timeout: float) -> Wakeup | None:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout)
        except (asyncio.TimeoutError, TimeoutError):
            return None

    def consume_overflow(self) -> bool:
        """Check-and-clear the overflow flag (set concurrently by push); drains on overflow."""
        if self.overflowed:
            self.overflowed = False
            self.drain()
            return True
        return False

    def drain(self) -> None:
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return


@dataclass(slots=True)
class Coalescer:
    """Dedup buffer: one ``Event`` per payload, counting arrivals."""

    pending: dict[str, Event] = field(init=False, default_factory=dict)

    def add(self, wakeup: Wakeup) -> None:
        prev = self.pending.get(wakeup.payload)
        if prev is None:
            self.pending[wakeup.payload] = Event(payload=wakeup.payload, first_seen=wakeup.at)
        else:
            self.pending[wakeup.payload] = Event(
                payload=prev.payload, first_seen=prev.first_seen, count=prev.count + 1
            )

    def flush(self) -> Batch:
        """Return the buffered window as a ``Batch`` and reset."""
        batch = Batch(tuple(self.pending.values()))
        self.pending.clear()
        return batch


@dataclass(frozen=True, slots=True, kw_only=True)
class Debouncer:
    """Window policy: rolling ``debounce`` quiet period, hard-capped at ``max_batch_wait``."""

    debounce: float
    max_batch_wait: float

    async def next_item(self, intake: Intake) -> FeedItem:
        """Collect one window from ``intake``; overflow yields ``Resync("overflow")``."""
        wakeup = await intake.get()
        if intake.consume_overflow():
            return Resync("overflow")

        coalescer = Coalescer()
        coalescer.add(wakeup)
        hard_deadline = time.monotonic() + self.max_batch_wait
        while True:
            remaining = min(self.debounce, hard_deadline - time.monotonic())
            if remaining <= 0:
                break
            more = await intake.get_within(remaining)
            if more is None:
                break
            coalescer.add(more)

        if intake.consume_overflow():
            return Resync("overflow")
        return coalescer.flush()


@dataclass(frozen=True, slots=True)
class Backoff:
    """Jittered exponential reconnect delay."""

    initial: float = 0.1
    maximum: float = 5.0

    def delay(self, attempt: int) -> float:
        base = min(self.maximum, self.initial * (2.0 ** min(attempt - 1, 16)))
        return base * random.uniform(0.5, 1.5)


@dataclass(slots=True, kw_only=True)
class FeedService:
    """Manages the moving parts: intake -> debouncer -> output, tasks, shutdown."""

    intake: Intake
    debouncer: Debouncer
    failsafe: float | None = None
    out: asyncio.Queue[FeedItem | None] = field(init=False, repr=False)  # None = closed
    tasks: list[asyncio.Task[None]] = field(init=False, default_factory=list)
    started: bool = field(init=False, default=False)
    closing: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self.out = asyncio.Queue()

    # -- transport side --

    def push(self, payload: str) -> None:
        self.intake.push(payload)

    def emit(self, item: FeedItem) -> None:
        self.out.put_nowait(item)

    # -- lifecycle --

    def start(self, supervisor: Callable[[], Coroutine[None, None, None]], name: str) -> None:
        if self.started:
            return
        self.started = True
        self.tasks.append(asyncio.create_task(supervisor(), name=f"{name}-supervisor"))
        self.tasks.append(asyncio.create_task(self._pump(), name=f"{name}-pump"))
        if self.failsafe is not None:
            self.tasks.append(asyncio.create_task(self._failsafe_loop(self.failsafe), name=f"{name}-failsafe"))

    async def aclose(self) -> None:
        if self.closing:
            return
        self.closing = True
        for t in self.tasks:
            t.cancel()
        for t in self.tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self.out.put_nowait(None)

    # -- consumer side --

    async def next_item(self) -> FeedItem | None:
        """Next item, or ``None`` once closed."""
        return await self.out.get()

    # -- internal loops --

    async def _pump(self) -> None:
        while True:
            self.emit(await self.debouncer.next_item(self.intake))

    async def _failsafe_loop(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            self.emit(Resync("failsafe"))


class BaseFeed:
    """Async-iterator surface over a ``FeedService``; subclasses provide the transport.

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
    ) -> None:
        self._service = FeedService(
            intake=Intake(maxsize=raw_queue_size),
            debouncer=Debouncer(
                debounce=debounce,
                max_batch_wait=max_batch_wait if max_batch_wait is not None else debounce * 20,
            ),
            failsafe=failsafe,
        )
        self._backoff = Backoff(initial=backoff[0], maximum=backoff[1])
        self.connection_pid: int | None = None  # server backend pid while connected

    # -- lifecycle --

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
        self._service.start(self._supervisor, name=type(self).__name__)

    async def aclose(self) -> None:
        if self._service.closing:
            return
        await self._service.aclose()
        await self._extra_close()
        self.connection_pid = None

    async def _extra_close(self) -> None:  # pragma: no cover - subclass hook
        return

    # -- consumer side --

    def __aiter__(self) -> AsyncIterator[FeedItem]:
        self._ensure_started()
        return self

    async def __anext__(self) -> FeedItem:
        item = await self._service.next_item()
        if item is None:
            raise StopAsyncIteration
        return item

    # -- transport-facing helpers --

    @property
    def _closing(self) -> bool:
        return self._service.closing

    def _push_raw(self, payload: str) -> None:
        self._service.push(payload)

    def _emit_resync(self, reason: str) -> None:
        self._service.emit(Resync(reason))

    def _backoff_delay(self, attempt: int) -> float:
        return self._backoff.delay(attempt)

    async def _supervisor(self) -> None:  # pragma: no cover - abstract
        raise NotImplementedError
