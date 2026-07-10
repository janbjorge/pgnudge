"""Unit tests for the engine classes: pure asyncio, no PostgreSQL."""

import asyncio
import logging

import pytest

from pgnudge.core import Batch, Resync
from pgnudge.engine import (
    TRACE,
    Backoff,
    BaseFeed,
    Coalescer,
    Debouncer,
    FeedService,
    Intake,
    Wakeup,
    trace_frame,
    validate_feed_params,
)
from pgnudge.proto import XLogData


def wakeup(payload: str, at: float = 1.0) -> Wakeup:
    return Wakeup(payload=payload, at=at)


# -- trace_frame --------------------------------------------------------------


def test_trace_frame_formats_the_frame_when_trace_is_enabled(caplog: pytest.LogCaptureFixture) -> None:
    log = logging.getLogger("pgnudge.test-trace")
    msg = XLogData(start_lsn=0x1_0000_0000, end_lsn=0x1_0000_0010, payload=b"public.orders")
    with caplog.at_level(TRACE, logger=log.name):
        trace_frame(log, msg, "-> %s", ["public.orders"])
    assert any("XLogData 1/0..1/10" in r.message and "public.orders" in r.message for r in caplog.records)


def test_trace_frame_is_silent_when_trace_is_off(caplog: pytest.LogCaptureFixture) -> None:
    log = logging.getLogger("pgnudge.test-trace-off")
    msg = XLogData(start_lsn=0, end_lsn=0, payload=b"x")
    with caplog.at_level(logging.DEBUG, logger=log.name):  # DEBUG is above TRACE
        trace_frame(log, msg, "-> %s", [])
    assert caplog.records == []


# -- validate_feed_params -----------------------------------------------------


def test_validate_feed_params_accepts_valid() -> None:
    validate_feed_params(status_interval=10.0, liveness_timeout=30.0, tables=["public.t"])
    validate_feed_params(status_interval=10.0, liveness_timeout=None, tables=None)


def test_validate_feed_params_rejects_nonpositive_status_interval() -> None:
    with pytest.raises(ValueError, match="status_interval"):
        validate_feed_params(status_interval=0.0, liveness_timeout=None, tables=None)


def test_validate_feed_params_rejects_liveness_not_exceeding_status() -> None:
    with pytest.raises(ValueError, match="liveness_timeout"):
        validate_feed_params(status_interval=10.0, liveness_timeout=10.0, tables=None)


def test_validate_feed_params_rejects_empty_tables() -> None:
    with pytest.raises(ValueError, match="tables"):
        validate_feed_params(status_interval=10.0, liveness_timeout=None, tables=[])


# -- Coalescer ----------------------------------------------------------------


def test_coalescer_dedups_and_counts() -> None:
    c = Coalescer()
    c.add(wakeup("public.picks", at=1.0))
    c.add(wakeup("public.picks", at=2.0))
    c.add(wakeup("public.picks", at=3.0))
    batch = c.flush()
    assert len(batch.events) == 1
    assert batch.events[0].count == 3
    assert batch.events[0].first_seen == 1.0  # first arrival wins


def test_coalescer_keeps_arrival_order() -> None:
    c = Coalescer()
    for payload in ("b", "a", "c", "a"):
        c.add(wakeup(payload))
    assert c.flush().payloads() == ("b", "a", "c")


def test_coalescer_flush_resets() -> None:
    c = Coalescer()
    c.add(wakeup("t"))
    assert len(c.flush().events) == 1
    assert c.flush().events == ()


# -- Intake -------------------------------------------------------------------


async def test_intake_push_get_roundtrip() -> None:
    intake = Intake(maxsize=4)
    intake.push("public.picks")
    got = await intake.get()
    assert got.payload == "public.picks"


async def test_intake_overflow_flags_and_drains() -> None:
    intake = Intake(maxsize=2)
    for _ in range(3):
        intake.push("t")
    assert intake.consume_overflow() is True  # flagged, queue drained
    assert await intake.get_within(0.05) is None
    assert intake.consume_overflow() is False  # cleared by the consume


async def test_intake_get_within_times_out() -> None:
    intake = Intake(maxsize=4)
    assert await intake.get_within(0.05) is None


# -- Debouncer ------------------------------------------------------------------


async def test_debouncer_emits_batch_after_quiet_period() -> None:
    intake = Intake(maxsize=64)
    deb = Debouncer(debounce=0.05, max_batch_wait=1.0)
    intake.push("public.picks")
    intake.push("public.picks")
    item = await asyncio.wait_for(deb.next_item(intake), 1.0)
    assert isinstance(item, Batch)
    assert item.payloads() == ("public.picks",)
    assert item.events[0].count == 2


async def test_debouncer_overflow_yields_resync() -> None:
    intake = Intake(maxsize=1)
    deb = Debouncer(debounce=0.05, max_batch_wait=1.0)
    intake.push("t")
    intake.push("t")  # overflows
    item = await asyncio.wait_for(deb.next_item(intake), 1.0)
    assert item == Resync("overflow")


async def test_debouncer_zero_max_batch_wait_closes_window_immediately() -> None:
    intake = Intake(maxsize=4)
    deb = Debouncer(debounce=1.0, max_batch_wait=0.0)
    intake.push("t")
    item = await asyncio.wait_for(deb.next_item(intake), 1.0)  # hard deadline, not debounce
    assert isinstance(item, Batch)
    assert item.payloads() == ("t",)


async def test_debouncer_overflow_during_open_window_yields_resync() -> None:
    intake = Intake(maxsize=1)
    deb = Debouncer(debounce=0.1, max_batch_wait=1.0)
    intake.push("a")
    window = asyncio.create_task(deb.next_item(intake))
    await asyncio.sleep(0.02)  # window is open, queue drained
    intake.push("b")
    intake.push("c")  # overflows mid-window
    assert await asyncio.wait_for(window, 1.0) == Resync("overflow")


async def test_debouncer_hard_deadline_caps_rolling_window() -> None:
    intake = Intake(maxsize=1024)
    deb = Debouncer(debounce=0.1, max_batch_wait=0.3)

    async def firehose() -> None:
        while True:
            intake.push("t")
            await asyncio.sleep(0.02)  # always inside the rolling debounce

    pump = asyncio.create_task(firehose())
    try:
        item = await asyncio.wait_for(deb.next_item(intake), 1.0)  # must close by max_batch_wait
    finally:
        pump.cancel()
    assert isinstance(item, Batch)
    assert item.events[0].count > 1


# -- Backoff --------------------------------------------------------------------


def test_backoff_first_attempt_jitters_around_initial() -> None:
    b = Backoff(initial=0.1, maximum=5.0)
    for _ in range(200):
        assert 0.05 <= b.delay(1) <= 0.15


def test_backoff_grows_then_caps_at_maximum() -> None:
    b = Backoff(initial=0.1, maximum=5.0)
    assert all(2.0 * 0.5 <= b.delay(6) <= 3.2 * 1.5 for _ in range(200))  # 0.1 * 2^5
    for attempt in (10, 100, 10_000):  # exponent clamped, capped at maximum
        for _ in range(200):
            assert b.delay(attempt) <= 5.0 * 1.5


# -- FeedService ------------------------------------------------------------------


def service(*, failsafe: float | None = None) -> FeedService:
    return FeedService(
        intake=Intake(maxsize=64),
        debouncer=Debouncer(debounce=0.03, max_batch_wait=0.5),
        failsafe=failsafe,
    )


async def test_service_pumps_pushes_into_batches() -> None:
    svc = service()

    async def transport() -> None:
        svc.emit(Resync("connected"))
        svc.push("public.picks")
        svc.push("public.picks")
        await asyncio.sleep(3600)

    svc.start(transport, name="test")
    try:
        assert await asyncio.wait_for(svc.next_item(), 1.0) == Resync("connected")
        item = await asyncio.wait_for(svc.next_item(), 1.0)
        assert isinstance(item, Batch)
        assert item.events[0].count == 2
    finally:
        await svc.aclose()


async def test_service_failsafe_emits_periodic_resync() -> None:
    svc = service(failsafe=0.05)

    async def transport() -> None:
        await asyncio.sleep(3600)

    svc.start(transport, name="test")
    try:
        assert await asyncio.wait_for(svc.next_item(), 1.0) == Resync("failsafe")
        assert await asyncio.wait_for(svc.next_item(), 1.0) == Resync("failsafe")
    finally:
        await svc.aclose()


async def test_service_start_is_idempotent() -> None:
    svc = service()

    async def transport() -> None:
        await asyncio.sleep(3600)

    svc.start(transport, name="test")
    svc.start(transport, name="test")  # no-op, no duplicate tasks
    assert len(svc.tasks) == 2  # supervisor + pump
    await svc.aclose()


async def test_service_close_yields_none_and_is_idempotent() -> None:
    svc = service()

    async def transport() -> None:
        await asyncio.sleep(3600)

    svc.start(transport, name="test")
    await svc.aclose()
    await svc.aclose()
    assert svc.closing is True
    assert await asyncio.wait_for(svc.next_item(), 1.0) is None


async def test_service_aclose_survives_crashed_task() -> None:
    svc = service()

    async def transport() -> None:
        raise RuntimeError("supervisor died")

    svc.start(transport, name="test")
    await asyncio.sleep(0.01)  # let the crash land before shutdown
    await svc.aclose()  # must not re-raise; the sentinel must still be queued
    assert await asyncio.wait_for(svc.next_item(), 1.0) is None


async def test_service_dead_task_queues_sentinel_without_aclose() -> None:
    # The hang the hardening targets: a task dies with an uncaught exception
    # and nobody calls aclose(). The done-callback must queue the sentinel so
    # a consumer blocked on next_item() wakes instead of blocking forever.
    svc = service()

    async def transport() -> None:
        raise RuntimeError("supervisor died")

    svc.start(transport, name="test")
    assert await asyncio.wait_for(svc.next_item(), 1.0) is None  # never hangs
    assert isinstance(svc.error, RuntimeError)
    await svc.aclose()


async def test_service_clean_task_return_is_not_an_error() -> None:
    # A supervisor that returns (rather than raising) is not a failure: the
    # done-callback must not record an error or queue a spurious sentinel.
    svc = service()

    async def transport() -> None:
        svc.emit(Resync("connected"))  # returns cleanly, no exception

    svc.start(transport, name="test")
    assert await asyncio.wait_for(svc.next_item(), 1.0) == Resync("connected")
    await asyncio.sleep(0.01)  # let the done-callback run
    assert svc.error is None
    await svc.aclose()


# -- BaseFeed ---------------------------------------------------------------------


class FakeFeed(BaseFeed):
    """Transport stub: one resync, a burst of wakeups, then idle."""

    def __init__(self) -> None:
        super().__init__(debounce=0.03)
        self.closes = 0

    async def _supervisor(self) -> None:
        self._emit_resync("connected")
        for _ in range(5):
            self._push_raw("public.picks")
        await asyncio.sleep(3600)

    async def _extra_close(self) -> None:
        self.closes += 1


class CrashFeed(BaseFeed):
    """Transport stub: emits one resync, then dies with an uncaught exception."""

    def __init__(self) -> None:
        super().__init__(debounce=0.03)

    async def _supervisor(self) -> None:
        self._emit_resync("connected")
        raise RuntimeError("boom")


async def test_basefeed_reraises_uncaught_task_death(caplog: pytest.LogCaptureFixture) -> None:
    feed = CrashFeed()
    with caplog.at_level(logging.ERROR, logger="pgnudge"):
        async with feed:
            # the buffered item is delivered first, then the failure surfaces
            assert await asyncio.wait_for(anext(feed), 1.0) == Resync("connected")
            with pytest.raises(RuntimeError, match="boom"):
                await asyncio.wait_for(anext(feed), 1.0)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1  # logged exactly once


async def test_basefeed_reraises_task_death_only_once() -> None:
    feed = CrashFeed()
    async with feed:
        await asyncio.wait_for(anext(feed), 1.0)  # drain the resync
        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(anext(feed), 1.0)
        # re-raised once; the closed feed then stops cleanly, never hangs
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(feed), 1.0)


async def test_basefeed_normal_close_records_no_error(caplog: pytest.LogCaptureFixture) -> None:
    feed = FakeFeed()
    with caplog.at_level(logging.ERROR, logger="pgnudge"):
        async with feed:
            await asyncio.wait_for(anext(feed), 1.0)
    assert feed._service.error is None  # cancellation on close is not an error
    assert [r for r in caplog.records if r.levelno == logging.ERROR] == []


async def test_basefeed_iterates_resync_then_batch_and_closes() -> None:
    feed = FakeFeed()
    async with feed:
        assert await asyncio.wait_for(anext(feed), 1.0) == Resync("connected")
        item = await asyncio.wait_for(anext(feed), 1.0)
        assert isinstance(item, Batch)
        assert item.events[0].count == 5
    assert feed.closes == 1
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(feed), 1.0)


async def test_basefeed_anext_after_close_raises_every_time() -> None:
    feed = FakeFeed()
    async with feed:
        await asyncio.wait_for(anext(feed), 1.0)
    for _ in range(2):  # second round has no sentinel left; must not hang
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(feed), 1.0)


async def test_basefeed_aclose_is_idempotent() -> None:
    feed = FakeFeed()
    async with feed:
        await asyncio.wait_for(anext(feed), 1.0)
    await feed.aclose()  # second close: no-op, _extra_close not re-run
    assert feed.closes == 1


async def test_basefeed_aiter_starts_without_context_manager() -> None:
    feed = FakeFeed()
    try:
        assert aiter(feed) is feed  # __aiter__ starts the service lazily
        assert await asyncio.wait_for(anext(feed), 1.0) == Resync("connected")
    finally:
        await feed.aclose()


async def test_basefeed_backoff_delegates() -> None:
    feed = FakeFeed()
    assert 0.05 <= feed._backoff_delay(1) <= 0.15
    await feed.aclose()


# -- connect-failure escalation ---------------------------------------------------


def test_connect_failure_sets_last_error_and_escalates_once(caplog: pytest.LogCaptureFixture) -> None:
    feed = FakeFeed()
    exc = ConnectionError("nope")
    with caplog.at_level(logging.ERROR, logger="pgnudge"):
        for _ in range(feed.CONNECT_ERROR_THRESHOLD * 3):  # keep failing past the threshold
            feed._record_connect_failure(exc)
    assert feed.last_error is exc
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1  # escalated exactly once, not once per attempt
    assert str(feed.CONNECT_ERROR_THRESHOLD) in errors[0].message


def test_connect_success_clears_error_and_allows_reescalation(caplog: pytest.LogCaptureFixture) -> None:
    feed = FakeFeed()
    with caplog.at_level(logging.ERROR, logger="pgnudge"):
        for _ in range(feed.CONNECT_ERROR_THRESHOLD):
            feed._record_connect_failure(ConnectionError("first streak"))
        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1
        feed._record_connect_success()
        assert feed.last_error is None  # cleared on a successful connect
        caplog.clear()
        for _ in range(feed.CONNECT_ERROR_THRESHOLD):  # a fresh streak escalates again
            feed._record_connect_failure(ConnectionError("second streak"))
        assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1
