"""CLI unit tests: arg parsing, verbosity mapping, env fallback, the
consume loop, and the observability taps; no PostgreSQL.
"""

import argparse
import logging
from types import TracebackType
from typing import Self

import pytest

from pgnudge import Batch, Event, RawFeed, Resync, WalFeed
from pgnudge.core import FeedItem
from pgnudge.doctor import Check, Diagnosis
from pgnudge.engine import TRACE, BaseFeed
from pgnudge.__main__ import _build_feed, _format_batch, _run_doctor, _verbosity_level, _watch, build_parser, main
from pgnudge.proto import payload_preview


# -- argument parsing ---------------------------------------------------------


def test_watch_transports_resolve() -> None:
    parser = build_parser()
    assert parser.parse_args(["watch", "logical"]).transport_kind == "logical"
    assert parser.parse_args(["watch", "physical"]).transport_kind == "physical"


def test_transport_is_nested_under_watch() -> None:
    # transport is not a top-level command; it must sit under `watch`
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["logical"])


def test_plugin_only_on_logical() -> None:
    parser = build_parser()
    assert parser.parse_args(["watch", "logical", "--plugin", "test_decoding"]).plugin == "test_decoding"
    with pytest.raises(SystemExit):
        parser.parse_args(["watch", "physical", "--plugin", "wal2json"])


def test_table_is_repeatable() -> None:
    args = build_parser().parse_args(["watch", "physical", "--table", "public.a", "--table", "public.b"])
    assert args.table == ["public.a", "public.b"]


def test_port_is_int() -> None:
    args = build_parser().parse_args(["watch", "logical", "--port", "6432"])
    assert args.port == 6432


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("require", True),
        ("verify-ca", True),
        ("verify-full", True),
        ("prefer", False),  # libpq would try TLS here; pgnudge does not
        ("allow", False),
        ("disable", False),
        ("", False),
    ],
)
def test_ssl_defaults_from_pgsslmode(mode: str, expected: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGSSLMODE", mode)
    assert build_parser().parse_args(["doctor"]).ssl is expected


def test_ssl_default_when_pgsslmode_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGSSLMODE", raising=False)
    assert build_parser().parse_args(["doctor"]).ssl is False


def test_ssl_help_documents_pgsslmode_interpretation() -> None:
    # A libpq-habituated user must learn from --help how their PGSSLMODE maps,
    # especially that prefer stays plaintext (a deviation from libpq).
    parser = build_parser()
    subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    help_text = subparsers.choices["doctor"].format_help()
    assert "verify-full" in help_text
    assert "prefer" in help_text


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
    args = build_parser().parse_args(["watch", "physical"])
    assert args.host == "envhost"
    assert args.user == "envuser"
    assert args.port == 7000


def test_flag_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGHOST", "envhost")
    args = build_parser().parse_args(["watch", "physical", "--host", "flaghost"])
    assert args.host == "flaghost"


def test_invalid_pgport_env_errors_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    # a bad PGPORT must be a parser error, not an int() traceback
    monkeypatch.setenv("PGPORT", "abc")
    with pytest.raises(SystemExit):
        build_parser().parse_args(["doctor"])


def test_empty_pgport_env_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGPORT", "")
    assert build_parser().parse_args(["doctor"]).port == 5432


def test_missing_user_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.setenv("PGDATABASE", "d")
    with pytest.raises(SystemExit):
        main(["watch", "physical"])


def test_missing_database_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGUSER", "u")
    monkeypatch.delenv("PGDATABASE", raising=False)
    with pytest.raises(SystemExit):
        main(["watch", "physical"])


def test_no_command_errors() -> None:
    with pytest.raises(SystemExit):
        main([])


def test_watch_without_transport_errors() -> None:
    with pytest.raises(SystemExit):
        main(["watch"])


# -- feed construction --------------------------------------------------------


def test_build_feed_selects_transport() -> None:
    parser = build_parser()
    wal = _build_feed(parser.parse_args(["watch", "logical", "--user", "u", "--database", "d"]))
    raw = _build_feed(parser.parse_args(["watch", "physical", "--user", "u", "--database", "d"]))
    assert isinstance(wal, WalFeed)
    assert isinstance(raw, RawFeed)


def test_aliases_map_to_transport() -> None:
    parser = build_parser()
    assert isinstance(_build_feed(parser.parse_args(["watch", "wal", "--user", "u", "--database", "d"])), WalFeed)
    assert isinstance(_build_feed(parser.parse_args(["watch", "raw", "--user", "u", "--database", "d"])), RawFeed)


def test_build_feed_passes_tables() -> None:
    args = build_parser().parse_args(
        ["watch", "physical", "--user", "u", "--database", "d", "--table", "public.orders"]
    )
    feed = _build_feed(args)
    assert isinstance(feed, RawFeed)
    assert feed.tables == frozenset({"public.orders"})


# -- doctor -------------------------------------------------------------------


def test_doctor_command_parses_with_plugin_default() -> None:
    args = build_parser().parse_args(["doctor", "--user", "u", "--database", "d"])
    assert args.command == "doctor"
    assert args.plugin == "wal2json"


def test_doctor_plugin_is_selectable() -> None:
    args = build_parser().parse_args(["doctor", "--user", "u", "--database", "d", "--plugin", "test_decoding"])
    assert args.plugin == "test_decoding"


def _fake_diagnose(recommended: str | None) -> object:
    async def diagnose(**kwargs: object) -> Diagnosis:
        return Diagnosis(
            checks=(
                Check("connect", True, "connected"),
                Check("WalFeed (logical decoding)", recommended is not None, "x", fix="run the magic command"),
            ),
            recommended=recommended,
        )

    return diagnose


async def test_run_doctor_prints_checks_and_recommendation(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("pgnudge.__main__.diagnose", _fake_diagnose("WalFeed"))
    args = build_parser().parse_args(["doctor", "--user", "u", "--database", "d"])
    ok = await _run_doctor(args)
    assert ok
    out = capsys.readouterr().out
    assert "[ok] connect: connected" in out
    assert "recommended transport: WalFeed" in out
    assert "fix:" not in out  # a passing check never prints its fix


async def test_run_doctor_reports_no_transport(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("pgnudge.__main__.diagnose", _fake_diagnose(None))
    args = build_parser().parse_args(["doctor", "--user", "u", "--database", "d"])
    ok = await _run_doctor(args)
    assert not ok
    out = capsys.readouterr().out
    assert "[FAIL] WalFeed (logical decoding)" in out
    assert "fix: run the magic command" in out  # a failed check surfaces its remediation
    assert "no transport available" in out


def test_main_doctor_exits_zero_when_a_transport_works(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pgnudge.__main__.diagnose", _fake_diagnose("RawFeed"))
    with pytest.raises(SystemExit) as exc:
        main(["doctor", "--user", "u", "--database", "d"])
    assert exc.value.code == 0


def test_main_doctor_exits_one_when_no_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pgnudge.__main__.diagnose", _fake_diagnose(None))
    with pytest.raises(SystemExit) as exc:
        main(["doctor", "--user", "u", "--database", "d"])
    assert exc.value.code == 1


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

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def __aiter__(self) -> Self:
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
