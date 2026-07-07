"""XLogWalker unit tests against synthetic WAL streams, no PostgreSQL.

``WalStream`` is a miniature WAL writer: it lays records out with the same
page headers, continuation flags, and MAXALIGN padding the server uses, so
framing edge cases can be built byte-exactly. The live pg_waldump oracle
test (test_raw.py) cross-checks the same decoder against real WAL.
"""

import logging
import struct

import pytest

from pgnudge.xlog import CommitGate, RelChange, TxnEnd, WalEvent, WalSyncError, XLogWalker

BLCKSZ = 8192
SEG = 16 * 1024 * 1024
MAGIC = 0xD116
REL = (1663, 5, 16384)


def record(
    *,
    rmid: int,
    info: int,
    xid: int = 100,
    blocks: bytes = b"",
    block_data: bytes = b"",
    main: bytes = b"",
) -> bytes:
    main_hdr = b""
    if main:
        if len(main) < 256:
            main_hdr = bytes([255, len(main)])
        else:
            main_hdr = bytes([254]) + struct.pack("<I", len(main))
    body = blocks + main_hdr + block_data + main
    tot_len = 24 + len(body)
    header = struct.pack("<IIQBBHI", tot_len, xid, 0, info, rmid, 0, 0)
    return header + body


def block_ref(
    *,
    block_id: int = 0,
    fork: int = 0,
    rel: tuple[int, int, int] | None = REL,
    data_len: int = 0,
    image: bytes = b"",
    bimg_info: int = 0,
) -> bytes:
    fork_flags = fork
    if data_len:
        fork_flags |= 0x20  # BKPBLOCK_HAS_DATA
    if image:
        fork_flags |= 0x10  # BKPBLOCK_HAS_IMAGE
    if rel is None:
        fork_flags |= 0x80  # BKPBLOCK_SAME_REL
    out = struct.pack("<BBH", block_id, fork_flags, data_len)
    if image:
        out += struct.pack("<HHB", len(image), 0, bimg_info)
        if bimg_info & 0x01 and bimg_info & 0x1C:
            out += struct.pack("<H", 42)  # XLogRecordBlockCompressHeader
    if rel is not None:
        out += struct.pack("<III", *rel)
    out += struct.pack("<I", 7)  # BlockNumber
    return out


def heap_insert(xid: int = 100, rel: tuple[int, int, int] = REL, data_len: int = 20) -> bytes:
    return record(
        rmid=10, info=0x00, xid=xid, blocks=block_ref(rel=rel, data_len=data_len), block_data=b"x" * data_len
    )


def commit(xid: int = 100, subxids: tuple[int, ...] = ()) -> bytes:
    if not subxids:
        return record(rmid=1, info=0x00, xid=xid, main=struct.pack("<Q", 0))
    main = struct.pack("<QI", 0, 0x02)  # xact_time, xinfo = HAS_SUBXACTS
    main += struct.pack(f"<i{len(subxids)}I", len(subxids), *subxids)
    return record(rmid=1, info=0x00 | 0x80, xid=xid, main=main)  # XLOG_XACT_HAS_INFO


class WalStream:
    """Miniature WAL writer: page headers, continuation flags, alignment."""

    def __init__(self, start: int, *, magic: int = MAGIC, seg: int = SEG) -> None:
        assert start % BLCKSZ == 0
        self.start = start
        self.pos = start
        self.magic = magic
        self.seg = seg
        self.out = bytearray()

    def page_header(self, rem: int) -> bytes:
        info = 0x0001 if rem else 0
        long_hdr = self.pos % self.seg == 0
        if long_hdr:
            info |= 0x0002
        header = struct.pack("<HHIQI", self.magic, info, 1, self.pos, rem) + b"\x00" * 4
        if long_hdr:
            header += struct.pack("<QII", 0x1234, self.seg, BLCKSZ)
        return header

    def put(self, data: bytes, *, is_record: bool) -> None:
        i = 0
        while i < len(data):
            if self.pos % BLCKSZ == 0:
                # rem_len only counts a record continuing from a previous page
                header = self.page_header(len(data) - i if is_record and i > 0 else 0)
                self.out += header
                self.pos += len(header)
            n = min(BLCKSZ - self.pos % BLCKSZ, len(data) - i)
            self.out += data[i : i + n]
            self.pos += n
            i += n

    def add(self, rec: bytes) -> None:
        pad = (-self.pos) % 8
        if pad:
            self.put(b"\x00" * pad, is_record=False)
        self.put(rec, is_record=True)

    def add_switch_padding(self) -> None:
        """Zero fill (no page headers) up to the next segment boundary."""
        n = (-self.pos) % self.seg
        self.out += b"\x00" * n
        self.pos += n

    def slice_from(self, lsn: int) -> bytes:
        assert lsn % BLCKSZ == 0 and lsn >= self.start
        return bytes(self.out[lsn - self.start :])


def walk(stream: WalStream, *, start: int | None = None) -> list[WalEvent]:
    lsn = start if start is not None else stream.start
    walker = XLogWalker(start_lsn=lsn)
    return walker.feed(stream.slice_from(lsn))


def start_lsn(*, seg_aligned: bool = False) -> int:
    base = 5 * SEG
    return base if seg_aligned else base + 4 * BLCKSZ


# -- happy paths -------------------------------------------------------------------


def test_heap_insert_yields_relchange() -> None:
    s = WalStream(start_lsn())
    s.add(heap_insert(xid=77))
    assert walk(s) == [RelChange(xid=77, db_oid=5, relfilenode=16384, kind="insert")]


def test_parse_record_returns_the_event_directly() -> None:
    """Leaf contract: parse_record returns the record's event, or None."""
    walker = XLogWalker(start_lsn=start_lsn())
    assert walker.parse_record(heap_insert(xid=9)) == RelChange(xid=9, db_oid=5, relfilenode=16384, kind="insert")
    assert walker.parse_record(commit(xid=9)) == TxnEnd(xid=9, committed=True)
    assert walker.parse_record(record(rmid=3, info=0x00)) is None  # unhandled resource manager


@pytest.mark.parametrize(
    ("info", "kind"),
    [(0x10, "delete"), (0x20, "update"), (0x40, "hot_update")],
)
def test_heap_ops_map_to_kinds(info: int, kind: str) -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=info, blocks=block_ref(data_len=4), block_data=b"abcd"))
    assert walk(s) == [RelChange(xid=100, db_oid=5, relfilenode=16384, kind=kind)]


def test_heap_init_page_flag_does_not_hide_the_op() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00 | 0x80, blocks=block_ref(data_len=4), block_data=b"abcd"))
    assert walk(s) == [RelChange(xid=100, db_oid=5, relfilenode=16384, kind="insert")]


def test_heap2_multi_insert_yields_relchange() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=9, info=0x50, blocks=block_ref(data_len=8), block_data=b"12345678"))
    assert walk(s) == [RelChange(xid=100, db_oid=5, relfilenode=16384, kind="multi_insert")]


def test_heap2_prune_is_ignored() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=9, info=0x10, blocks=block_ref(data_len=4), block_data=b"abcd"))
    assert walk(s) == []


def test_other_rmgrs_are_ignored() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=11, info=0x00, blocks=block_ref(data_len=4), block_data=b"abcd"))  # btree
    assert walk(s) == []


def test_commit_without_info_flag() -> None:
    s = WalStream(start_lsn())
    s.add(commit(xid=9))
    assert walk(s) == [TxnEnd(xid=9, committed=True)]


def test_commit_with_subxids() -> None:
    s = WalStream(start_lsn())
    s.add(commit(xid=9, subxids=(10, 11)))
    assert walk(s) == [TxnEnd(xid=9, committed=True, subxids=(10, 11))]


def test_commit_with_dbinfo_then_subxids() -> None:
    main = struct.pack("<QI", 0, 0x01 | 0x02) + struct.pack("<II", 5, 1663)  # dbinfo
    main += struct.pack("<i2I", 2, 20, 21)
    s = WalStream(start_lsn())
    s.add(record(rmid=1, info=0x80, xid=9, main=main))
    assert walk(s) == [TxnEnd(xid=9, committed=True, subxids=(20, 21))]


def test_commit_with_dbinfo_but_no_subxids() -> None:
    main = struct.pack("<QI", 0, 0x01) + struct.pack("<II", 5, 1663)
    s = WalStream(start_lsn())
    s.add(record(rmid=1, info=0x80, xid=9, main=main))
    assert walk(s) == [TxnEnd(xid=9, committed=True)]


def test_abort_yields_uncommitted_txnend() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=1, info=0x20, xid=9, main=struct.pack("<Q", 0)))
    assert walk(s) == [TxnEnd(xid=9, committed=False)]


def test_prepare_emits_committed_immediately() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=1, info=0x10, xid=9))
    assert walk(s) == [TxnEnd(xid=9, committed=True)]


def test_commit_prepared_and_assignment_are_ignored() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=1, info=0x30, xid=9, main=struct.pack("<Q", 0)))
    s.add(record(rmid=1, info=0x50, xid=9, main=b"\x00" * 8))
    assert walk(s) == []


# -- framing -----------------------------------------------------------------------


def test_record_spanning_page_boundary() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=block_ref(data_len=300), block_data=b"x" * 300, main=b"m" * 12000))
    s.add(heap_insert(xid=42))
    events = walk(s)
    assert [e.xid for e in events if isinstance(e, RelChange)] == [100, 42]


def test_stream_started_mid_record_skips_to_next_whole_record() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, xid=1, blocks=block_ref(data_len=100), block_data=b"x" * 100, main=b"m" * 20000))
    s.add(heap_insert(xid=2))
    later = start_lsn() + 2 * BLCKSZ  # inside the big first record
    assert walk(s, start=later) == [RelChange(xid=2, db_oid=5, relfilenode=16384, kind="insert")]


def test_long_page_header_at_segment_start_updates_seg_size() -> None:
    s = WalStream(start_lsn(seg_aligned=True))
    s.add(heap_insert(xid=3))
    walker = XLogWalker(start_lsn=s.start)
    events = walker.feed(s.slice_from(s.start))
    assert events == [RelChange(xid=3, db_oid=5, relfilenode=16384, kind="insert")]
    assert walker.seg_size == SEG


def test_byte_at_a_time_feeding_matches_single_feed() -> None:
    s = WalStream(start_lsn())
    s.add(heap_insert(xid=1))
    s.add(commit(xid=1, subxids=(2,)))
    s.add(record(rmid=10, info=0x20, xid=3, blocks=block_ref(data_len=64), block_data=b"y" * 64, main=b"m" * 9000))
    whole = XLogWalker(start_lsn=s.start).feed(s.slice_from(s.start))
    dribble = XLogWalker(start_lsn=s.start)
    events: list[WalEvent] = []
    stream = bytes(s.out)
    for i in range(len(stream)):
        events += dribble.feed(stream[i : i + 1])
    assert events == whole and len(whole) == 3


def test_emit_from_suppresses_records_already_written_at_attach() -> None:
    s = WalStream(start_lsn())
    s.add(heap_insert(xid=1))
    boundary = s.pos  # everything up to here is history
    s.add(heap_insert(xid=2))
    walker = XLogWalker(start_lsn=s.start, emit_from=boundary)
    assert walker.feed(s.slice_from(s.start)) == [RelChange(xid=2, db_oid=5, relfilenode=16384, kind="insert")]


def test_xlog_switch_skips_headerless_zero_fill() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=0, info=0x40))  # XLOG_SWITCH
    s.add_switch_padding()
    s.add(heap_insert(xid=8))
    assert walk(s) == [RelChange(xid=8, db_oid=5, relfilenode=16384, kind="insert")]


def test_fpi_with_compressed_hole_is_skipped_correctly() -> None:
    image = b"i" * 96
    s = WalStream(start_lsn())
    s.add(
        record(
            rmid=10,
            info=0x20,
            xid=6,
            blocks=block_ref(data_len=10, image=image, bimg_info=0x01 | 0x04),
            block_data=image + b"d" * 10,
        )
    )
    assert walk(s) == [RelChange(xid=6, db_oid=5, relfilenode=16384, kind="update")]


def test_uncompressed_fpi_is_skipped_correctly() -> None:
    image = b"i" * 64
    s = WalStream(start_lsn())
    s.add(
        record(
            rmid=10,
            info=0x00,
            xid=6,
            blocks=block_ref(data_len=10, image=image, bimg_info=0x01),  # hole, not compressed
            block_data=image + b"d" * 10,
        )
    )
    assert walk(s) == [RelChange(xid=6, db_oid=5, relfilenode=16384, kind="insert")]


def test_same_rel_second_block_reuses_locator() -> None:
    blocks = block_ref(block_id=0, data_len=4) + block_ref(block_id=1, rel=None, data_len=4)
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x20, xid=6, blocks=blocks, block_data=b"aaaabbbb"))
    assert walk(s) == [RelChange(xid=6, db_oid=5, relfilenode=16384, kind="update")]


def test_max_block_id_reference_is_accepted() -> None:
    """XLR_MAX_BLOCK_ID (32) is the top valid block reference, not a special marker."""
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=block_ref(block_id=32, data_len=4), block_data=b"abcd"))
    assert walk(s) == [RelChange(xid=100, db_oid=5, relfilenode=16384, kind="insert")]


def test_non_main_fork_block_yields_nothing() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=block_ref(fork=2, data_len=4), block_data=b"abcd"))
    assert walk(s) == []


def test_long_main_data_header() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=1, info=0x00, xid=5, main=struct.pack("<Q", 0) + b"\x00" * 300))
    assert walk(s) == [TxnEnd(xid=5, committed=True)]


def test_origin_and_toplevel_xid_headers_are_skipped() -> None:
    extra = bytes([253]) + struct.pack("<H", 1) + bytes([252]) + struct.pack("<I", 99)
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, xid=7, blocks=extra + block_ref(data_len=4), block_data=b"abcd"))
    assert walk(s) == [RelChange(xid=7, db_oid=5, relfilenode=16384, kind="insert")]


def test_unknown_magic_warns_once_and_decodes(caplog: pytest.LogCaptureFixture) -> None:
    s = WalStream(start_lsn(), magic=0xD1FF)
    s.add(heap_insert(xid=1))
    s.add(record(rmid=10, info=0x00, xid=2, blocks=block_ref(data_len=4), block_data=b"abcd", main=b"m" * 9000))
    with caplog.at_level(logging.WARNING, logger="pgnudge.xlog"):
        events = walk(s)
    assert [e.xid for e in events if isinstance(e, RelChange)] == [1, 2]
    assert sum("unknown WAL page magic" in r.message for r in caplog.records) == 1


# -- desync and validation ----------------------------------------------------------


def test_unaligned_start_lsn_is_rejected() -> None:
    with pytest.raises(ValueError, match="page-aligned"):
        XLogWalker(start_lsn=start_lsn() + 1)


def test_page_floor_aligns_down() -> None:
    assert XLogWalker.page_floor(start_lsn() + 5000) == start_lsn()
    assert XLogWalker.page_floor(start_lsn()) == start_lsn()


def test_bad_magic_family_raises() -> None:
    s = WalStream(start_lsn(), magic=0xAB01)
    s.add(heap_insert())
    with pytest.raises(WalSyncError, match="bad page magic"):
        walk(s)


def test_pageaddr_mismatch_raises() -> None:
    s = WalStream(start_lsn())
    s.add(heap_insert())
    walker = XLogWalker(start_lsn=start_lsn() + BLCKSZ)  # wrong position on purpose
    with pytest.raises(WalSyncError, match="does not match stream position"):
        walker.feed(s.slice_from(s.start))


def test_unsupported_block_size_raises() -> None:
    s = WalStream(start_lsn(seg_aligned=True))
    s.add(heap_insert())
    data = bytearray(s.slice_from(s.start))
    struct.pack_into("<I", data, 36, 4096)  # xlp_xlog_blcksz inside the long header
    with pytest.raises(WalSyncError, match="block size"):
        XLogWalker(start_lsn=s.start).feed(bytes(data))


def test_implausible_record_length_raises() -> None:
    s = WalStream(start_lsn())
    s.add(heap_insert())
    data = bytearray(s.slice_from(s.start))
    struct.pack_into("<I", data, 24, 3)  # tot_len < header size
    with pytest.raises(WalSyncError, match="implausible record length"):
        XLogWalker(start_lsn=s.start).feed(bytes(data))


def test_continuation_flag_mismatch_raises() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=block_ref(data_len=100), block_data=b"x" * 100, main=b"m" * 12000))
    data = bytearray(s.slice_from(s.start))
    struct.pack_into("<H", data, BLCKSZ + 2, 0)  # clear xlp_info on the continuation page
    with pytest.raises(WalSyncError, match="continuation flag mismatch"):
        XLogWalker(start_lsn=s.start).feed(bytes(data))


def test_invalid_block_reference_id_raises() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=bytes([200]) + b"\x00" * 20))
    with pytest.raises(WalSyncError, match="invalid block reference id"):
        walk(s)


def test_block_id_just_above_max_raises() -> None:
    """One past XLR_MAX_BLOCK_ID (33) is neither a reference nor a marker."""
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=block_ref(block_id=33, data_len=4), block_data=b"abcd"))
    with pytest.raises(WalSyncError, match="invalid block reference id"):
        walk(s)


def test_same_rel_without_prior_block_raises() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=block_ref(rel=None, data_len=4), block_data=b"abcd"))
    with pytest.raises(WalSyncError, match="SAME_REL without a prior block"):
        walk(s)


def test_truncated_block_header_raises() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=bytes([0, 0x20])))  # header cut short
    with pytest.raises(WalSyncError, match="malformed record header|overran"):
        walk(s)


def test_header_walk_overrun_raises() -> None:
    s = WalStream(start_lsn())
    s.add(record(rmid=10, info=0x00, blocks=block_ref(data_len=50), block_data=b"x" * 10))
    with pytest.raises(WalSyncError, match="overran"):
        walk(s)


def test_malformed_xact_subxids_raises() -> None:
    main = struct.pack("<QI", 0, 0x02) + struct.pack("<i", 100)  # claims 100 subxids, has none
    s = WalStream(start_lsn())
    s.add(record(rmid=1, info=0x80, xid=9, main=main))
    with pytest.raises(WalSyncError, match="malformed xact record"):
        walk(s)


# -- commit gate ---------------------------------------------------------------------


def change(xid: int, relfilenode: int = 16384, kind: str = "insert") -> RelChange:
    return RelChange(xid=xid, db_oid=5, relfilenode=relfilenode, kind=kind)


def test_gate_releases_only_on_commit() -> None:
    gate = CommitGate()
    assert gate.push([change(1)]) == []
    assert gate.push([TxnEnd(xid=1, committed=True)]) == [change(1)]
    assert gate.pending == {}


def test_gate_drops_aborted_transactions() -> None:
    gate = CommitGate()
    assert gate.push([change(1), TxnEnd(xid=1, committed=False)]) == []
    assert gate.pending == {}


def test_gate_merges_subtransaction_changes_on_commit() -> None:
    gate = CommitGate()
    assert gate.push([change(11, relfilenode=1), change(12, relfilenode=2)]) == []
    released = gate.push([TxnEnd(xid=10, committed=True, subxids=(11, 12))])
    assert {c.relfilenode for c in released} == {1, 2}


def test_gate_dedups_repeat_changes_within_a_transaction() -> None:
    gate = CommitGate()
    gate.push([change(1, kind="insert"), change(1, kind="update"), change(1, kind="delete")])
    released = gate.push([TxnEnd(xid=1, committed=True)])
    assert released == [change(1, kind="insert")]  # first change per (db, rel) wins


def test_gate_isolates_interleaved_transactions() -> None:
    gate = CommitGate()
    gate.push([change(1, relfilenode=1), change(2, relfilenode=2)])
    assert gate.push([TxnEnd(xid=2, committed=True)]) == [change(2, relfilenode=2)]
    assert gate.push([TxnEnd(xid=1, committed=False)]) == []


def test_gate_ignores_txnend_for_unknown_xid() -> None:
    assert CommitGate().push([TxnEnd(xid=99, committed=True)]) == []


def test_gate_evicts_oldest_open_transaction_as_committed() -> None:
    gate = CommitGate(max_open=2)
    released = gate.push([change(1), change(2), change(3)])
    assert released == [change(1)]  # oldest evicted, emitted rather than lost
    assert set(gate.pending) == {2, 3}
