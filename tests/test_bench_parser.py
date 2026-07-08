"""Throughput benchmarks for the two parsers, no PostgreSQL.

Timed by pytest-benchmark (rounds, stats, comparison).

Run:     uv run pytest tests/test_bench_parser.py --benchmark-only
Compare: add --benchmark-autosave, then --benchmark-compare on a later run.
"""

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from pgnudge.wal import WalFeed
from pgnudge.xlog import XLogWalker
from test_xlog import REL, WalStream, commit, heap_insert

pytestmark = [
    pytest.mark.benchmark,  # deselect with -m "not benchmark"
    pytest.mark.timeout(0),  # opt out of the suite-wide 2 s cap
]

# ~32 MB of stream: large enough to swamp per-call overhead and touch many pages.
TARGET_BYTES = 32 * 1024 * 1024


def build_stream() -> tuple[bytes, int]:
    """A realistic mix: heap inserts, one commit per 10, laid out with real page
    framing. Returns (bytes, expected_event_count) — one event per record."""
    stream = WalStream(0)
    records = 0
    xid = 100
    while len(stream.out) < TARGET_BYTES:
        stream.add(heap_insert(xid=xid, rel=REL, data_len=40))
        records += 1
        if records % 10 == 0:
            stream.add(commit(xid=xid))
            records += 1
            xid += 1
    return bytes(stream.out), records


def test_bench_physical_walker(benchmark: BenchmarkFixture) -> None:
    """XLogWalker.feed: byte-level page/record/header walk end to end."""
    data, events = build_stream()

    # a fresh walker per pass: feed() mutates walker state, so it cannot be reused
    result = benchmark(lambda: XLogWalker(start_lsn=0).feed(data))

    assert len(result) == events, f"stream built {events} events, walker found {len(result)}"
    # ~32 MB/pass: OPS in the report x 32 = MB/s.
    benchmark.extra_info["stream_mb"] = round(len(data) / (1024 * 1024), 1)


@pytest.mark.parametrize(
    ("name", "payload"),
    [
        ("wal2json_v2", b'{"action":"U","schema":"public","table":"stations","columns":[]}'),
        ("test_decoding", b"table public.stations: UPDATE: id[integer]:1 name[text]:'st-1'"),
    ],
    ids=["wal2json_v2", "test_decoding"],
)
def test_bench_logical_parser(benchmark: BenchmarkFixture, name: str, payload: bytes) -> None:
    """The two payload->tables parsers, per-message rather than per-byte."""
    fn = WalFeed._parse_wal2json_v2 if name == "wal2json_v2" else WalFeed._parse_test_decoding

    result = benchmark(fn, payload)

    assert result == ["public.stations"]
