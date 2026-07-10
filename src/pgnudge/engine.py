"""The machinery behind a feed, one class per concern, pure stdlib.

``Intake`` buffers raw wakeups, ``Coalescer`` dedups them, ``Debouncer``
decides when a window closes, ``Backoff`` paces reconnects, and
``FeedService`` wires them together behind the async-iterator surface
that ``BaseFeed`` exposes.
"""

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import ClassVar, Self

from pgnudge.core import Batch, Event, FeedItem, Resync
from pgnudge.errors import ConfigError
from pgnudge.proto import StatusFeedback, WalsenderConnection, XLogData, format_lsn, payload_preview

__all__ = [
    "TRACE",
    "trace_frame",
    "validate_feed_params",
    "Wakeup",
    "Intake",
    "Coalescer",
    "Debouncer",
    "Backoff",
    "FeedService",
    "BaseFeed",
]

# One step below DEBUG: the per-frame / per-record firehose the CLI turns on
# at -vvv. Off by default, so the guarded taps that emit at this level cost
# nothing in normal use.
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


def trace_frame(log: logging.Logger, msg: XLogData, tail: str, *args: object) -> None:
    """TRACE-log one wire frame: shared prefix, transport-specific ``tail``.

    Guarded so the LSN/preview formatting never runs when TRACE is off.
    """
    if not log.isEnabledFor(TRACE):
        return
    log.log(
        TRACE,
        "XLogData %s..%s len=%d %r " + tail,
        format_lsn(msg.start_lsn),
        format_lsn(msg.end_lsn),
        len(msg.payload),
        payload_preview(msg.payload),
        *args,
    )


def validate_feed_params(
    *, status_interval: float, liveness_timeout: float | None, tables: list[str] | None
) -> None:
    """Validate the feedback/liveness/table knobs shared by every transport."""
    if status_interval <= 0:
        raise ConfigError("status_interval must be positive")
    if liveness_timeout is not None and liveness_timeout <= status_interval:
        raise ConfigError("liveness_timeout must exceed status_interval")
    if tables is not None and not tables:
        raise ConfigError("tables must be None or a non-empty list")


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
        # asyncio.timeout, not wait_for: 3.11's wait_for can swallow an
        # external cancel when the inner get() already has an item, leaving
        # the pump task uncancellable and aclose() hanging
        try:
            async with asyncio.timeout(timeout):
                return await self.queue.get()
        except TimeoutError:
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
            self.pending[wakeup.payload] = replace(prev, count=prev.count + 1)

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
    # Logger for the fatal-task path; BaseFeed wires its transport logger in.
    log: logging.Logger = field(default=logging.getLogger("pgnudge"))
    out: asyncio.Queue[FeedItem | None] = field(init=False, repr=False)  # None = closed
    tasks: list[asyncio.Task[None]] = field(init=False, default_factory=list)
    started: bool = field(init=False, default=False)
    closing: bool = field(init=False, default=False)
    # An uncaught task exception (a pgnudge bug, not a connection drop). Set
    # once by _on_task_done; re-raised once from BaseFeed.__anext__ so a dead
    # task terminates ``async for`` loudly instead of hanging on out.get().
    error: BaseException | None = field(init=False, default=None)
    error_delivered: bool = field(init=False, default=False)

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
            self.tasks.append(
                asyncio.create_task(self._failsafe_loop(self.failsafe), name=f"{name}-failsafe")
            )
        # Observe every task: without this an uncaught exception is invisible
        # until aclose(), so the consumer blocks on out.get() forever.
        for t in self.tasks:
            t.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        """Surface a task that died with an uncaught exception.

        Cancellation and any death during shutdown are expected; everything
        else is a bug in a supervisor/pump loop that must not masquerade as a
        quiet close. Capture it, log once, and queue the sentinel so the
        blocked consumer wakes and ``__anext__`` can re-raise.
        """
        if self.closing or task.cancelled():
            return
        exc = task.exception()
        if exc is None or self.error is not None:
            return
        self.error = exc
        self.log.error("feed task %s died; terminating the feed", task.get_name(), exc_info=exc)
        self.out.put_nowait(None)

    async def aclose(self) -> None:
        if self.closing:
            return
        self.closing = True
        for t in self.tasks:
            t.cancel()
        # gather, not per-task await: a task that died with a real exception
        # must not abort shutdown before the sentinel is queued
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.out.put_nowait(None)

    # -- consumer side --

    async def next_item(self) -> FeedItem | None:
        """Next item, or ``None`` once closed (or dead) and drained."""
        # A captured task error is terminal like a close: once the buffered
        # items and the sentinel are drained, stop returning so __anext__ ends
        # instead of blocking on an out.get() that will never complete.
        if (self.closing or self.error is not None) and self.out.empty():
            return None
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

    Failure semantics (two distinct modes):

    * A *connection* failure (drop, refused connect, auth retry) is the
      documented contract: the supervisor backs off and reconnects forever,
      re-emitting ``Resync`` on the next connect. It never surfaces on the
      iterator.
    * An *uncaught internal* exception in a supervisor/pump/failsafe task is a
      pgnudge bug, not an operational hiccup. It is logged once at ERROR and
      re-raised from ``__anext__``, so ``async for`` terminates with the real
      traceback instead of hanging silently. ``aclose()`` never re-raises.
    """

    # Subclasses override with their transport logger (pgnudge.wal / .raw);
    # the base default keeps ``_push_raw`` logging valid on its own.
    log: ClassVar[logging.Logger] = logging.getLogger("pgnudge")

    # Consecutive failed connects before escalating from per-attempt WARNING
    # to a single ERROR, so a permanently-failing feed (bad password,
    # unreachable host) is visible to an operator tailing ERROR instead of
    # lost in the retry noise. Retries themselves continue unchanged.
    CONNECT_ERROR_THRESHOLD: ClassVar[int] = 5

    def __init__(
        self,
        *,
        status_interval: float = 10.0,
        liveness_timeout: float | None = 30.0,
        connect_timeout: float = 10.0,
        tables: list[str] | None = None,
        debounce: float = 0.05,
        max_batch_wait: float | None = None,
        failsafe: float | None = None,
        backoff: tuple[float, float] = (0.1, 5.0),
        raw_queue_size: int = 8192,
    ) -> None:
        validate_feed_params(
            status_interval=status_interval, liveness_timeout=liveness_timeout, tables=tables
        )
        self._service = FeedService(
            intake=Intake(maxsize=raw_queue_size),
            debouncer=Debouncer(
                debounce=debounce,
                max_batch_wait=max_batch_wait if max_batch_wait is not None else debounce * 20,
            ),
            failsafe=failsafe,
            log=self.log,
        )
        self._backoff = Backoff(initial=backoff[0], maximum=backoff[1])
        self.connection_pid: int | None = None  # server backend pid while connected
        self.status_interval = status_interval
        self.liveness_timeout = liveness_timeout
        self.connect_timeout = connect_timeout
        self.last_inbound = time.monotonic()  # updated by the transport on every frame
        self.last_error: Exception | None = None  # last connect failure; None while healthy
        self._connect_failures = 0

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
            svc = self._service
            # A dead task (not a normal close) re-raises exactly once, so the
            # bug surfaces on the iterator with its original traceback instead
            # of the loop ending silently.
            if svc.error is not None and not svc.closing and not svc.error_delivered:
                svc.error_delivered = True
                raise svc.error
            raise StopAsyncIteration
        return item

    # -- transport-facing helpers --

    @property
    def _closing(self) -> bool:
        return self._service.closing

    def _push_raw(self, payload: str) -> None:
        # The convergence point for both transports: every committed
        # schema.table passes here. -vv (DEBUG) surfaces the named change.
        self.log.debug("nudge %s", payload)
        self._service.push(payload)

    def _emit_resync(self, reason: str) -> None:
        self._service.emit(Resync(reason))

    def _backoff_delay(self, attempt: int) -> float:
        return self._backoff.delay(attempt)

    async def _reconnect_pause(self, attempt: int) -> None:
        delay = self._backoff_delay(attempt)
        self.log.debug("reconnect attempt %d in %.2fs", attempt, delay)
        await asyncio.sleep(delay)

    def _record_connect_failure(self, exc: Exception) -> None:
        """Track a failed connect and escalate once if failures persist.

        Retry behavior is unchanged (the documented contract). This only
        exposes ``last_error`` for health checks and, on the
        ``CONNECT_ERROR_THRESHOLD``-th consecutive failure, emits a single
        ERROR so a dead-on-arrival feed is not buried in the per-attempt
        WARNING stream. Later failures in the same streak stay quiet; a
        successful connect resets the streak so a fresh one can escalate again.
        """
        self.last_error = exc
        self._connect_failures += 1
        if self._connect_failures == self.CONNECT_ERROR_THRESHOLD:
            self.log.error(
                "still cannot connect after %d attempts; last error: %s",
                self._connect_failures,
                exc,
            )

    def _record_connect_success(self) -> None:
        """Clear the connect-failure state once a connection is established."""
        self.last_error = None
        self._connect_failures = 0

    async def _feedback_loop(self, conn: WalsenderConnection) -> None:
        await StatusFeedback(
            conn=conn,
            interval=self.status_interval,
            liveness=self.liveness_timeout,
            lsn=self._current_lsn,
            idle=lambda: time.monotonic() - self.last_inbound,
            log=self.log,
        ).run()

    @contextlib.asynccontextmanager
    async def _feedback_running(self, conn: WalsenderConnection) -> AsyncIterator[None]:
        """Run the standby-status/liveness loop for the duration of the stream."""
        task = asyncio.create_task(self._feedback_loop(conn))
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def _current_lsn(self) -> int:  # pragma: no cover - abstract
        raise NotImplementedError

    async def _supervisor(self) -> None:  # pragma: no cover - abstract
        raise NotImplementedError
