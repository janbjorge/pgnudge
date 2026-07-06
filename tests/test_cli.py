"""CLI unit tests: arg parsing, verbosity mapping, env fallback, the
consume loop, and the observability taps; no PostgreSQL.
"""

from __future__ import annotations

import logging
from types import TracebackType

import pytest

from pgnudge import Batch, Event, RawFeed, Resync, WalFeed
from pgnudge.core import FeedItem
from pgnudge.engine import TRACE, BaseFeed
from pgnudge.__main__ import _build_feed, _format_batch, _verbosity_level, _watch, build_parser, main
from pgnudge.proto import payload_preview


# -- argument parsing ---------------------------------------------------------


def test_parser_wal_and_raw_subcommands() -> None:
    parser = build_parser()
    assert parser.parse_args(["wal"]).command == "wal"
    assert parser.parse_args(["raw"]).command == "raw"


def test_plugin_only_on_wal() -> None:
    parser = build_parser()
    assert parser.parse_args(["wal", "--plugin", "test_decoding"]).plugin == "test_decoding"
    with pytest.raises(SystemExit):
        parser.parse_args(["raw", "--plugin", "wal2json"])


def test_table_is_repeatable() -> None:
    args = build_parser().parse_args(["raw", "--table", "public.a", "--table", "public.b"])
    assert args.table == ["public.a", "public.b"]


def test_port_is_int() -> None:
    args = build_parser().parse_args(["wal", "--port", "6432"])
    assert args.port == 6432


# -- verbosity ----------------------------------------------------------------


def test_verbosity_ladder() -> None:
    assert _verbosity_level(0) == logging.WARNING
    assert _verbosity_level(1) == logging.INFO
    assert _verbosity_level(2) == logging.DEBUG
    assert _verbosity_level(3) == TRACE
    assert _verbosity_level(9) == TRACE  # saturates, never IndexError


def test_trace_level_registered() -> None:
    assert logging.getLevelName(TRACE) == "TRACE"


# -- environment fallback -----------------------------------------------------


def test_pg_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGHOST", "envhost")
    monkeypatch.setenv("PGUSER", "envuser")
    monkeypatch.setenv("PGPORT", "7000")
    args = build_parser().parse_args(["raw"])
    assert args.host == "envhost"
    assert args.user == "envuser"
    assert args.port == 7000


def test_flag_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGHOST", "envhost")
    args = build_parser().parse_args(["raw", "--host", "flaghost"])
    assert args.host == "flaghost"


def test_missing_user_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.setenv("PGDATABASE", "d")
    with pytest.raises(SystemExit):
        main(["raw"])


def test_missing_database_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGUSER", "u")
    monkeypatch.delenv("PGDATABASE", raising=False)
    with pytest.raises(SystemExit):
        main(["raw"])


# -- feed construction --------------------------------------------------------


def test_build_feed_selects_transport() -> None:
    parser = build_parser()
    wal = _build_feed(parser.parse_args(["wal", "--user", "u", "--database", "d"]))
    raw = _build_feed(parser.parse_args(["raw", "--user", "u", "--database", "d"]))
    assert isinstance(wal, WalFeed)
    assert isinstance(raw, RawFeed)


def test_build_feed_passes_tables() -> None:
    args = build_parser().parse_args(["raw", "--user", "u", "--database", "d", "--table", "public.orders"])
    feed = _build_feed(args)
    assert isinstance(feed, RawFeed)
    assert feed.tables == frozenset({"public.orders"})


# -- output formatting --------------------------------------------------------


def test_format_batch_marks_counts() -> None:
    batch = Batch(
        events=(
            Event(payload="public.orders", first_seen=1.0, count=3),
            Event(payload="public.picks", first_seen=1.0, count=1),
        )
    )
    assert _format_batch(batch) == "batch: public.orders (x3), public.picks"


# -- consume loop -------------------------------------------------------------


class ReplayFeed(BaseFeed):
    """A BaseFeed that replays a fixed item list, no transport, no service loop."""

    def __init__(self, items: list[FeedItem]) -> None:
        super().__init__()
        self._items = items

    async def __aenter__(self) -> ReplayFeed:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def __aiter__(self) -> ReplayFeed:
        self._it = iter(self._items)
        return self

    async def __anext__(self) -> FeedItem:
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration from None


async def test_watch_prints_resync_and_batch(capsys: pytest.CaptureFixture[str]) -> None:
    feed = ReplayFeed(
        [
            Resync("connected"),
            Batch(events=(Event(payload="public.orders", first_seen=1.0, count=2),)),
        ]
    )
    await _watch(feed)
    out = capsys.readouterr().out.splitlines()
    assert out == ["resync: connected", "batch: public.orders (x2)"]


# -- observability taps -------------------------------------------------------


def test_push_raw_logs_committed_name(caplog: pytest.LogCaptureFixture) -> None:
    feed = WalFeed(user="u", database="d")
    with caplog.at_level(logging.DEBUG, logger="pgnudge"):
        feed._push_raw("public.orders")
    assert any("nudge public.orders" in r.getMessage() for r in caplog.records)


def test_payload_preview_truncates() -> None:
    assert payload_preview(b"abc", limit=8) == "abc"
    assert payload_preview(b"abcdefghij", limit=4) == "abcd..."


def test_payload_preview_survives_non_utf8() -> None:
    # invalid UTF-8 must not raise; it renders with backslash escapes
    assert "\\x" in payload_preview(b"\xff\xfe", limit=8)
