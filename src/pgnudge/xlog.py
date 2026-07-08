"""Sans-io physical-WAL walker: which relation changed, never row contents.

Feed bytes from a ``START_REPLICATION PHYSICAL`` stream, collect
``RelChange`` and ``TxnEnd`` events. Only record headers and block
references are parsed (layouts stable since PostgreSQL 9.5); row
payloads and CRCs are skipped. Little-endian servers only. Mechanism
and scope: docs/physical-wal.md.
"""

import logging
import struct
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import ClassVar, TypeAlias

from pgnudge.errors import ConfigError, PgnudgeError

__all__ = [
    "CommitGate",
    "HeaderWalk",
    "RelChange",
    "RelFileLocator",
    "TxnEnd",
    "WalEvent",
    "WalSyncError",
    "XLogWalker",
]


class WalSyncError(PgnudgeError):
    """The walker lost the record framing; abort the stream and reconnect."""


@dataclass(frozen=True, slots=True)
class RelChange:
    """One heap change: transaction ``xid`` touched ``relfilenode`` in ``db_oid``."""

    xid: int
    db_oid: int
    relfilenode: int
    kind: str  # insert | delete | update | hot_update | multi_insert


@dataclass(frozen=True, slots=True)
class TxnEnd:
    """Transaction outcome; ``subxids`` are the committed/aborted subtransactions."""

    xid: int
    committed: bool
    subxids: tuple[int, ...] = ()


WalEvent: TypeAlias = RelChange | TxnEnd


@dataclass(frozen=True, slots=True)
class RelFileLocator:
    """Physical relation identity as WAL block references carry it."""

    spc_oid: int
    db_oid: int
    relnumber: int


@dataclass(frozen=True, slots=True)
class HeaderWalk:
    """Header-walk result: main-fork locator (lowest block) and main-data offset."""

    locator: RelFileLocator | None
    main_off: int


@dataclass(slots=True)
class RecordCursor:
    """Sequential reader over a WAL record; every read advances the position.

    Replaces the manual ``offset += n`` / ``remaining -= n`` pair the header
    walk used to keep in lockstep: the position lives in one place and
    ``remaining`` is derived, so the two can never drift. Out-of-bounds reads
    raise ``IndexError`` / ``struct.error``, which the caller maps to
    ``WalSyncError``.
    """

    rec: bytes
    off: int

    def u8(self) -> int:
        value = self.rec[self.off]
        self.off += 1
        return value

    def unpack(self, fmt: str) -> tuple[int, ...]:
        values: tuple[int, ...] = struct.unpack_from(fmt, self.rec, self.off)
        self.off += struct.calcsize(fmt)
        return values

    def skip(self, n: int) -> None:
        self.off += n

    @property
    def remaining(self) -> int:
        return len(self.rec) - self.off


@dataclass(slots=True, kw_only=True)
class CommitGate:
    """Holds changes per transaction; releases them only at commit.

    Physical WAL carries changes as they are written, before the outcome
    is known. Releasing at commit keeps parity with logical decoding: no
    wakeups for rollbacks, no consumer refetch racing an open transaction.
    """

    max_open: int = 4096
    pending: dict[int, dict[tuple[int, int], RelChange]] = field(init=False, default_factory=dict)

    log: ClassVar[logging.Logger] = logging.getLogger("pgnudge.xlog")

    def push(self, events: Iterable[WalEvent]) -> list[RelChange]:
        """Absorb walker events; return changes whose transaction committed."""
        released: list[RelChange] = []
        for event in events:
            if isinstance(event, RelChange):
                bucket = self.pending.setdefault(event.xid, {})
                bucket.setdefault((event.db_oid, event.relfilenode), event)
                if len(self.pending) > self.max_open:
                    # emit on eviction: a spurious nudge beats a lost one
                    evicted = next(iter(self.pending))
                    released.extend(self.pending.pop(evicted).values())
                    self.log.debug("evicted open transaction %d past max_open=%d", evicted, self.max_open)
            else:
                for xid in (event.xid, *event.subxids):
                    changes = self.pending.pop(xid, None)
                    if changes is not None and event.committed:
                        released.extend(changes.values())
        return released


@dataclass(slots=True, kw_only=True)
class XLogWalker:
    """Streaming record walker; construct at a page-aligned WAL position.

    ``feed`` buffers arbitrary byte chunks and returns events as records
    complete. The first page's ``xlp_rem_len`` skips any record already in
    flight at the start position, so any ``page_floor``-aligned LSN is a
    valid entry point. ``emit_from`` suppresses events from records ending
    at or before it: framing must start at a page boundary, but records
    already written when the caller attached are history, not news.
    """

    start_lsn: int
    emit_from: int = 0
    pos: int = field(init=False)
    buf: bytearray = field(init=False)
    skip: int = field(init=False, default=0)
    raw_skip: int = field(init=False, default=0)
    rec: bytearray | None = field(init=False, default=None)
    rec_need: int | None = field(init=False, default=None)
    first_page: bool = field(init=False, default=True)
    seg_size: int = field(init=False, default=16 * 1024 * 1024)
    warned_magic: bool = field(init=False, default=False)

    log: ClassVar[logging.Logger] = logging.getLogger("pgnudge.xlog")

    # precompiled once: struct.unpack_from(fmt, ...) reparses fmt every call
    _S_LEN: ClassVar[struct.Struct] = struct.Struct("<I")  # record/block-data length
    _S_REC_HDR: ClassVar[struct.Struct] = struct.Struct("<IIQBB")  # tot_len,xid,prev,info,rmid
    _S_PAGE_HDR: ClassVar[struct.Struct] = struct.Struct("<HHIQI")  # magic,info,tli,pageaddr,rem_len
    _S_LONG_TAIL: ClassVar[struct.Struct] = struct.Struct("<QII")  # sysid,seg_size,blcksz
    _S_XINFO: ClassVar[struct.Struct] = struct.Struct("<I")  # xact xinfo word
    _S_SUBXCNT: ClassVar[struct.Struct] = struct.Struct("<i")  # subxid count

    BLCKSZ: ClassVar[int] = 8192
    ALIGN: ClassVar[int] = 8
    SHORT_PHD: ClassVar[int] = 24
    LONG_PHD: ClassVar[int] = 40
    REC_HDR: ClassVar[int] = 24
    MAX_RECORD: ClassVar[int] = 1 << 30
    PAGE_MAGICS: ClassVar[frozenset[int]] = frozenset({0xD113, 0xD116, 0xD118})  # PG 16, 17, 18
    XLP_FIRST_IS_CONTRECORD: ClassVar[int] = 0x0001
    XLR_MAX_BLOCK_ID: ClassVar[int] = 32  # block-reference ids 0..32; 252..255 are special markers
    # Special block-reference ids and fork/image flag bits, named as in the
    # PostgreSQL source (xlogrecord.h) so a reader can grep the server code.
    XLR_BLOCK_ID_DATA_SHORT: ClassVar[int] = 255
    XLR_BLOCK_ID_DATA_LONG: ClassVar[int] = 254
    XLR_BLOCK_ID_ORIGIN: ClassVar[int] = 253
    XLR_BLOCK_ID_TOPLEVEL_XID: ClassVar[int] = 252
    BKPBLOCK_FORK_MASK: ClassVar[int] = 0x0F
    BKPBLOCK_HAS_IMAGE: ClassVar[int] = 0x10
    BKPBLOCK_SAME_REL: ClassVar[int] = 0x80
    BKIMG_HAS_HOLE: ClassVar[int] = 0x01
    BKIMG_COMPRESS_MASK: ClassVar[int] = 0x1C  # ZLIB | LZ4 | ZSTD bits

    RM_XLOG: ClassVar[int] = 0
    RM_XACT: ClassVar[int] = 1
    RM_HEAP2: ClassVar[int] = 9
    RM_HEAP: ClassVar[int] = 10
    XLOG_SWITCH: ClassVar[int] = 0x40
    HEAP_KINDS: ClassVar[dict[int, str]] = {
        0x00: "insert",
        0x10: "delete",
        0x20: "update",
        0x40: "hot_update",
    }
    HEAP2_MULTI_INSERT: ClassVar[int] = 0x50
    XACT_COMMIT: ClassVar[int] = 0x00
    XACT_PREPARE: ClassVar[int] = 0x10
    XACT_ABORT: ClassVar[int] = 0x20
    XACT_HAS_INFO: ClassVar[int] = 0x80
    XINFO_HAS_DBINFO: ClassVar[int] = 0x01
    XINFO_HAS_SUBXACTS: ClassVar[int] = 0x02

    def __post_init__(self) -> None:
        if self.start_lsn % self.BLCKSZ:
            raise ConfigError("start_lsn must be page-aligned; use XLogWalker.page_floor")
        self.pos = self.start_lsn
        self.buf = bytearray()

    @classmethod
    def page_floor(cls, lsn: int) -> int:
        """Round ``lsn`` down to a page boundary, the only valid walker start position."""
        return lsn - lsn % cls.BLCKSZ

    # -- streaming state machine -------------------------------------------------

    def feed(self, data: bytes) -> list[WalEvent]:
        """Buffer one stream chunk; return events for the records it completed."""
        self.buf += data
        return [event for rec in self.records() if (event := self.parse_record(rec)) is not None]

    def consume(self, n: int) -> None:
        # CPython bytearray tracks an internal start offset, so front deletion
        # is amortized O(1); no manual cursor needed.
        del self.buf[:n]
        self.pos += n

    def records(self) -> Iterator[bytes]:
        """Drain complete records from the buffer; return when more input is needed.

        Records are 8-byte aligned and interleaved with a page header every
        BLCKSZ bytes, so a record spanning pages is reassembled across the
        intervening headers; its total length is read from its first 4
        bytes, which may themselves straddle a page. ``skip`` crosses
        alignment padding and the in-flight record tail on the first page;
        ``raw_skip`` crosses XLOG_SWITCH zero fill, which has no page
        headers at all.
        """
        while self.buf:
            if self.raw_skip:  # zero fill after XLOG_SWITCH: no page headers inside
                take = min(self.raw_skip, len(self.buf))
                self.consume(take)
                self.raw_skip -= take
                continue
            if self.pos % self.BLCKSZ == 0:
                if not self.take_page_header():
                    return
                continue
            avail = min(len(self.buf), self.BLCKSZ - self.pos % self.BLCKSZ)
            if self.skip:
                take = min(self.skip, avail)
                self.consume(take)
                self.skip -= take
                continue
            if self.rec is None:
                pad = (-self.pos) % self.ALIGN
                if pad:
                    self.skip = pad
                    continue
                self.rec = bytearray()
                self.rec_need = None
            if self.rec_need is None:
                take = min(4 - len(self.rec), avail)
                self.rec += self.buf[:take]
                self.consume(take)
                if len(self.rec) == 4:
                    (tot_len,) = self._S_LEN.unpack_from(self.rec)
                    if tot_len < self.REC_HDR or tot_len > self.MAX_RECORD:
                        raise WalSyncError(f"implausible record length {tot_len} at 0x{self.pos:X}")
                    self.rec_need = tot_len
                continue
            take = min(self.rec_need - len(self.rec), avail)
            self.rec += self.buf[:take]
            self.consume(take)
            if len(self.rec) == self.rec_need:
                record = bytes(self.rec)
                self.rec = None
                self.rec_need = None
                if self.pos > self.emit_from:
                    yield record

    def take_page_header(self) -> bool:
        """Consume one page header; return False until it is fully buffered.

        The header doubles as the framing self-check: the page address must
        equal the stream position and the continuation flag must agree with
        whether a record is in flight, otherwise the walker declares desync
        rather than emit garbage. Long headers (segment start) carry the
        segment size that places later long headers and XLOG_SWITCH fills.
        The first page's ``xlp_rem_len`` skips the tail of a record already
        in flight at ``start_lsn``; that skip is what makes any page-aligned
        position a valid entry point.
        """
        size = self.LONG_PHD if self.pos % self.seg_size == 0 else self.SHORT_PHD
        if len(self.buf) < size:
            return False
        magic, info, _tli, pageaddr, rem_len = self._S_PAGE_HDR.unpack_from(self.buf)
        if magic >> 8 != 0xD1:
            raise WalSyncError(f"bad page magic 0x{magic:04X} at 0x{self.pos:X}")
        if magic not in self.PAGE_MAGICS and not self.warned_magic:
            self.warned_magic = True
            self.log.warning("unknown WAL page magic 0x%04X (new PostgreSQL major?); decoding anyway", magic)
        if pageaddr != self.pos:
            raise WalSyncError(f"page address 0x{pageaddr:X} does not match stream position 0x{self.pos:X}")
        if size == self.LONG_PHD:
            _sysid, seg_size, blcksz = self._S_LONG_TAIL.unpack_from(self.buf, 24)
            if blcksz != self.BLCKSZ:
                raise WalSyncError(f"unsupported WAL block size {blcksz}")
            self.seg_size = seg_size
        continuation = bool(info & self.XLP_FIRST_IS_CONTRECORD)
        if self.first_page:
            self.first_page = False
            self.skip = rem_len  # tail of a record already in flight at our start
        elif continuation != (self.rec is not None or self.skip > 0):
            raise WalSyncError(f"continuation flag mismatch at 0x{self.pos:X}")
        self.consume(size)
        return True

    # -- record parsing ------------------------------------------------------------

    def parse_record(self, rec: bytes) -> WalEvent | None:
        """Decode one complete record into an event; None for record types that never nudge."""
        _tot_len, xid, _prev, info, rmid = self._S_REC_HDR.unpack_from(rec)
        op = info & 0x70
        if rmid == self.RM_XACT:
            return self.parse_xact(rec, xid, info, op)
        if rmid == self.RM_HEAP and op in self.HEAP_KINDS:
            return self.parse_heap(rec, xid, self.HEAP_KINDS[op])
        if rmid == self.RM_HEAP2 and op == self.HEAP2_MULTI_INSERT:
            return self.parse_heap(rec, xid, "multi_insert")
        if rmid == self.RM_XLOG and op == self.XLOG_SWITCH:
            # rest of the segment is zero padding without page headers
            self.raw_skip = (-self.pos) % self.seg_size
        return None

    def parse_heap(self, rec: bytes, xid: int, kind: str) -> RelChange | None:
        locator = self.walk_headers(rec).locator
        if locator is None:
            return None
        return RelChange(xid=xid, db_oid=locator.db_oid, relfilenode=locator.relnumber, kind=kind)

    def parse_xact(self, rec: bytes, xid: int, info: int, op: int) -> TxnEnd | None:
        """Decode a transaction outcome, collecting subxids when present.

        The main data of a commit/abort record starts with the 8-byte
        xact_time; an ``xinfo`` flag word follows only under XACT_HAS_INFO,
        and each xinfo bit appends its section in a fixed order, so the
        dbinfo section must be skipped over to reach the subxid array.
        """
        if op == self.XACT_PREPARE:
            # emit at PREPARE time: a spurious nudge on a later rollback is
            # acceptable, a nudge stranded until eviction is not
            return TxnEnd(xid=xid, committed=True)
        if op not in (self.XACT_COMMIT, self.XACT_ABORT):
            return None
        main = rec[self.walk_headers(rec).main_off :]
        subxids: tuple[int, ...] = ()
        if info & self.XACT_HAS_INFO and len(main) >= 12:
            try:
                (xinfo,) = self._S_XINFO.unpack_from(main, 8)  # follows the 8-byte xact_time
                offset = 12
                if xinfo & self.XINFO_HAS_DBINFO:
                    offset += 8
                if xinfo & self.XINFO_HAS_SUBXACTS:
                    (count,) = self._S_SUBXCNT.unpack_from(main, offset)
                    offset += 4
                    subxids = struct.unpack_from(f"<{count}I", main, offset)
            except struct.error as exc:
                raise WalSyncError(f"malformed xact record: {exc}") from exc
        return TxnEnd(xid=xid, committed=op == self.XACT_COMMIT, subxids=subxids)

    def walk_headers(self, rec: bytes) -> HeaderWalk:
        """Walk the block-reference headers of one record.

        Headers and data live in separate areas: every block reference and
        main-data marker declares its payload length up front, and the
        payloads follow only after the last header. Walking the headers
        alone therefore yields the main-fork locator and the main-data
        offset without ever decoding row contents; the final
        ``remaining != datatotal`` check proves the walk stayed in sync.
        """
        cursor = RecordCursor(rec, self.REC_HDR)
        datatotal = 0
        main_len = 0
        locator: RelFileLocator | None = None
        last: RelFileLocator | None = None
        try:
            while cursor.remaining > datatotal:
                block_id = cursor.u8()
                if block_id == self.XLR_BLOCK_ID_DATA_SHORT:
                    main_len = cursor.u8()
                    datatotal += main_len
                elif block_id == self.XLR_BLOCK_ID_DATA_LONG:
                    (main_len,) = cursor.unpack("<I")
                    datatotal += main_len
                elif block_id == self.XLR_BLOCK_ID_ORIGIN:
                    cursor.skip(2)
                elif block_id == self.XLR_BLOCK_ID_TOPLEVEL_XID:
                    cursor.skip(4)
                elif block_id <= self.XLR_MAX_BLOCK_ID:
                    fork_flags = cursor.u8()
                    (data_len,) = cursor.unpack("<H")
                    datatotal += data_len
                    if fork_flags & self.BKPBLOCK_HAS_IMAGE:
                        img_len, _hole_offset = cursor.unpack("<HH")
                        bimg_info = cursor.u8()
                        if bimg_info & self.BKIMG_HAS_HOLE and bimg_info & self.BKIMG_COMPRESS_MASK:
                            cursor.skip(2)  # XLogRecordBlockCompressHeader
                        datatotal += img_len
                    if fork_flags & self.BKPBLOCK_SAME_REL:
                        if last is None:
                            raise WalSyncError("BKPBLOCK_SAME_REL without a prior block")
                        this = last
                    else:
                        spc_oid, db_oid, relnumber = cursor.unpack("<III")
                        this = RelFileLocator(spc_oid=spc_oid, db_oid=db_oid, relnumber=relnumber)
                    cursor.skip(4)  # BlockNumber
                    last = this
                    if locator is None and fork_flags & self.BKPBLOCK_FORK_MASK == 0:  # main fork only
                        locator = this
                else:
                    raise WalSyncError(f"invalid block reference id {block_id}")
        except (IndexError, struct.error) as exc:
            raise WalSyncError(f"malformed record header: {exc}") from exc
        if cursor.remaining != datatotal:
            raise WalSyncError("record header walk overran the data area")
        return HeaderWalk(locator=locator, main_off=len(rec) - main_len)
