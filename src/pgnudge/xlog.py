"""Sans-io physical-WAL walker: which relation changed, never row contents.

Feed bytes from a ``START_REPLICATION PHYSICAL`` stream, collect
``RelChange`` and ``TxnEnd`` events. Only record headers and block
references are parsed (layouts stable since PostgreSQL 9.5); row
payloads and CRCs are skipped. Little-endian servers only. Mechanism
and scope: docs/physical-wal.md.
"""

import logging
import struct
from dataclasses import dataclass, field
from typing import ClassVar, TypeAlias

__all__ = ["RelChange", "TxnEnd", "WalEvent", "WalSyncError", "XLogWalker"]


class WalSyncError(Exception):
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


@dataclass(slots=True, kw_only=True)
class XLogWalker:
    """Streaming record walker; construct at a page-aligned WAL position.

    ``feed`` buffers arbitrary byte chunks and returns events as records
    complete. The first page's ``xlp_rem_len`` skips any record already in
    flight at the start position, so any ``page_floor``-aligned LSN is a
    valid entry point.
    """

    start_lsn: int
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

    BLCKSZ: ClassVar[int] = 8192
    ALIGN: ClassVar[int] = 8
    SHORT_PHD: ClassVar[int] = 24
    LONG_PHD: ClassVar[int] = 40
    REC_HDR: ClassVar[int] = 24
    MAX_RECORD: ClassVar[int] = 1 << 30
    PAGE_MAGICS: ClassVar[frozenset[int]] = frozenset({0xD113, 0xD116})  # PG 16, 17
    XLP_FIRST_IS_CONTRECORD: ClassVar[int] = 0x0001

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
            raise ValueError("start_lsn must be page-aligned; use XLogWalker.page_floor")
        self.pos = self.start_lsn
        self.buf = bytearray()

    @classmethod
    def page_floor(cls, lsn: int) -> int:
        return lsn - lsn % cls.BLCKSZ

    # -- streaming state machine -------------------------------------------------

    def feed(self, data: bytes) -> list[WalEvent]:
        self.buf += data
        out: list[WalEvent] = []
        while self.step(out):
            pass
        return out

    def consume(self, n: int) -> None:
        del self.buf[:n]
        self.pos += n

    def step(self, out: list[WalEvent]) -> bool:
        """One state-machine transition; False when more input is needed."""
        if not self.buf:
            return False
        if self.raw_skip:  # zero fill after XLOG_SWITCH: no page headers inside
            take = min(self.raw_skip, len(self.buf))
            self.consume(take)
            self.raw_skip -= take
            return True
        if self.pos % self.BLCKSZ == 0:
            return self.take_page_header()
        avail = min(len(self.buf), self.BLCKSZ - self.pos % self.BLCKSZ)
        if self.skip:
            take = min(self.skip, avail)
            self.consume(take)
            self.skip -= take
            return True
        if self.rec is None:
            pad = (-self.pos) % self.ALIGN
            if pad:
                self.skip = pad
                return True
            self.rec = bytearray()
            self.rec_need = None
        if self.rec_need is None:
            take = min(4 - len(self.rec), avail)
            self.rec += self.buf[:take]
            self.consume(take)
            if len(self.rec) == 4:
                (tot_len,) = struct.unpack("<I", bytes(self.rec))
                if tot_len < self.REC_HDR or tot_len > self.MAX_RECORD:
                    raise WalSyncError(f"implausible record length {tot_len} at 0x{self.pos:X}")
                self.rec_need = tot_len
            return True
        take = min(self.rec_need - len(self.rec), avail)
        self.rec += self.buf[:take]
        self.consume(take)
        if len(self.rec) == self.rec_need:
            record = bytes(self.rec)
            self.rec = None
            self.rec_need = None
            self.parse_record(record, out)
        return True

    def take_page_header(self) -> bool:
        size = self.LONG_PHD if self.pos % self.seg_size == 0 else self.SHORT_PHD
        if len(self.buf) < size:
            return False
        magic, info, _tli, pageaddr, rem_len = struct.unpack_from("<HHIQI", self.buf)
        if magic >> 8 != 0xD1:
            raise WalSyncError(f"bad page magic 0x{magic:04X} at 0x{self.pos:X}")
        if magic not in self.PAGE_MAGICS and not self.warned_magic:
            self.warned_magic = True
            self.log.warning("unknown WAL page magic 0x%04X (new PostgreSQL major?); decoding anyway", magic)
        if pageaddr != self.pos:
            raise WalSyncError(f"page address 0x{pageaddr:X} does not match stream position 0x{self.pos:X}")
        if size == self.LONG_PHD:
            _sysid, seg_size, blcksz = struct.unpack_from("<QII", self.buf, 24)
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

    def parse_record(self, rec: bytes, out: list[WalEvent]) -> None:
        _tot_len, xid, _prev, info, rmid = struct.unpack_from("<IIQBB", rec)
        op = info & 0x70
        if rmid == self.RM_XACT:
            self.parse_xact(rec, xid, info, out)
        elif rmid == self.RM_HEAP and op in self.HEAP_KINDS:
            self.emit_main_fork_change(rec, xid, self.HEAP_KINDS[op], out)
        elif rmid == self.RM_HEAP2 and op == self.HEAP2_MULTI_INSERT:
            self.emit_main_fork_change(rec, xid, "multi_insert", out)
        elif rmid == self.RM_XLOG and op == self.XLOG_SWITCH:
            # rest of the segment is zero padding without page headers
            self.raw_skip = (-self.pos) % self.seg_size

    def emit_main_fork_change(self, rec: bytes, xid: int, kind: str, out: list[WalEvent]) -> None:
        locator, _main_off, _main_len = self.walk_headers(rec)
        if locator is not None:
            _spc_oid, db_oid, relnumber = locator
            out.append(RelChange(xid=xid, db_oid=db_oid, relfilenode=relnumber, kind=kind))

    def parse_xact(self, rec: bytes, xid: int, info: int, out: list[WalEvent]) -> None:
        op = info & 0x70
        if op == self.XACT_PREPARE:
            # emit at PREPARE time: a spurious nudge on a later rollback is
            # acceptable, a nudge stranded until eviction is not
            out.append(TxnEnd(xid=xid, committed=True))
            return
        if op not in (self.XACT_COMMIT, self.XACT_ABORT):
            return
        _locator, main_off, main_len = self.walk_headers(rec)
        subxids: tuple[int, ...] = ()
        if info & self.XACT_HAS_INFO and main_len >= 12:
            main = rec[main_off : main_off + main_len]
            try:
                (xinfo,) = struct.unpack_from("<I", main, 8)  # follows the 8-byte xact_time
                offset = 12
                if xinfo & self.XINFO_HAS_DBINFO:
                    offset += 8
                if xinfo & self.XINFO_HAS_SUBXACTS:
                    (count,) = struct.unpack_from("<i", main, offset)
                    offset += 4
                    subxids = struct.unpack_from(f"<{count}I", main, offset)
            except struct.error as exc:
                raise WalSyncError(f"malformed xact record: {exc}") from exc
        out.append(TxnEnd(xid=xid, committed=op == self.XACT_COMMIT, subxids=subxids))

    def walk_headers(self, rec: bytes) -> tuple[tuple[int, int, int] | None, int, int]:
        """Walk the block-reference headers; return (main-fork locator of the
        lowest block, main-data offset, main-data length)."""
        offset = self.REC_HDR
        remaining = len(rec) - offset
        datatotal = 0
        main_len = 0
        locator: tuple[int, int, int] | None = None
        last: tuple[int, int, int] | None = None
        try:
            while remaining > datatotal:
                block_id = rec[offset]
                offset += 1
                remaining -= 1
                if block_id == 255:  # XLR_BLOCK_ID_DATA_SHORT
                    main_len = rec[offset]
                    offset += 1
                    remaining -= 1
                    datatotal += main_len
                elif block_id == 254:  # XLR_BLOCK_ID_DATA_LONG
                    (main_len,) = struct.unpack_from("<I", rec, offset)
                    offset += 4
                    remaining -= 4
                    datatotal += main_len
                elif block_id == 253:  # XLR_BLOCK_ID_ORIGIN
                    offset += 2
                    remaining -= 2
                elif block_id == 252:  # XLR_BLOCK_ID_TOPLEVEL_XID
                    offset += 4
                    remaining -= 4
                elif block_id <= 31:
                    fork_flags = rec[offset]
                    (data_len,) = struct.unpack_from("<H", rec, offset + 1)
                    offset += 3
                    remaining -= 3
                    datatotal += data_len
                    if fork_flags & 0x10:  # BKPBLOCK_HAS_IMAGE
                        (img_len, _hole_offset) = struct.unpack_from("<HH", rec, offset)
                        bimg_info = rec[offset + 4]
                        offset += 5
                        remaining -= 5
                        if bimg_info & 0x01 and bimg_info & 0x1C:  # hole + compressed
                            offset += 2  # XLogRecordBlockCompressHeader
                            remaining -= 2
                        datatotal += img_len
                    if fork_flags & 0x80:  # BKPBLOCK_SAME_REL
                        if last is None:
                            raise WalSyncError("BKPBLOCK_SAME_REL without a prior block")
                        this = last
                    else:
                        spc_oid, db_oid, relnumber = struct.unpack_from("<III", rec, offset)
                        offset += 12
                        remaining -= 12
                        this = (spc_oid, db_oid, relnumber)
                    offset += 4  # BlockNumber
                    remaining -= 4
                    last = this
                    if locator is None and fork_flags & 0x0F == 0:  # main fork only
                        locator = this
                else:
                    raise WalSyncError(f"invalid block reference id {block_id}")
        except (IndexError, struct.error) as exc:
            raise WalSyncError(f"malformed record header: {exc}") from exc
        if remaining != datatotal:
            raise WalSyncError("record header walk overran the data area")
        return locator, len(rec) - main_len, main_len
