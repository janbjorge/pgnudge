# Parsing: from wire bytes to `schema.table`

How pgnudge turns replication traffic into payload strings, at the
structure level: the frames, the record layouts, and the dataclasses
each layer produces. Companion to
[temporary-slots.md](temporary-slots.md) (why the logical transport is
shaped this way) and [physical-wal.md](physical-wal.md) (why the
physical one is); this page is what the parsers actually read.

## The pipeline at a glance

Two transports, two parsing stacks, one convergence point. Everything
downstream of the transports handles plain `schema.table` strings.

```
proto.py     wire frames ────────────────► XLogData.payload (bytes)
  logical    wal.py       parse plugin text ──────────► "schema.table"
  physical   xlog.py      XLogWalker ► CommitGate ► RelChange
             raw.py       RelResolver: relfilenode ───► "schema.table"
  both       engine.py    Intake ► Coalescer ► Debouncer
  contract   core.py      Batch | Resync
```

The split is who decodes the WAL. On the logical path the server
decodes and pgnudge parses short text messages. On the physical path
the server ships raw WAL bytes and pgnudge does the decoding itself,
headers only.

## The wire layer (`proto.py`)

Both transports speak the streaming-replication protocol through
`WalsenderConnection`. Every backend message is a type byte plus a
big-endian `int32` length; `read_stream()` extracts two frame types
from CopyData (`d`) messages and returns them as frozen dataclasses:

| CopyData subtype | layout after the subtype byte | becomes |
|---|---|---|
| `w` XLogData | `!QQQ` start_lsn, end_lsn, send-time; payload follows | `XLogData(start_lsn, end_lsn, payload)` |
| `k` keepalive | `!QQB` end_lsn, send-time, reply-requested | `Keepalive(end_lsn, reply_requested)` |

Every frame nests the same way: the message envelope wraps a CopyData
byte, which wraps a subtype byte, which prefixes a fixed struct. All
fields big-endian. The envelope length counts itself plus the body but
not the leading type byte, so the body is `length - 4`.

```
message envelope (all backend messages)
+------+-------------------+========================================+
| type |     length        |                body                    |
| 'd'  |     int32         |     (CopyData payload, below)          |
+------+-------------------+========================================+
  1 B         4 B               length - 4 bytes
              \___ counts length field + body, not the type byte ___/

'w' XLogData  (server to client, carries WAL)
+------+----------+----------+----------+=====================+
| 'w'  | start_lsn|  end_lsn | send_time|      payload        |
+------+----------+----------+----------+=====================+
  1 B     8 B  Q    8 B  Q     8 B  Q     rest of body -> XLogData.payload
          \__ struct.unpack !QQQ, body[1:25] __/   body[25:]

'k' Keepalive  (server to client, may demand a reply)
+------+----------+----------+-------+
| 'k'  |  end_lsn | send_time| reply |
+------+----------+----------+-------+
  1 B     8 B  Q    8 B  Q     1 B B
          \__ struct.unpack !QQB, body[1:18] __/

'r' Standby status update  (client to server, the only reply frame)
+------+------+----------+----------+----------+----------+-------+
| 'd'  | 'r'  |  written | flushed  | applied  |timestamp | reply |
+------+------+----------+----------+----------+----------+-------+
 env.    1 B    8 B  Q     8 B  Q     8 B  Q     8 B  Q     1 B B
         \______ struct.pack !QQQQB; same LSN thrice, then ts, flag _/
```

Send-times are skipped. `XLogData.start_lsn` is the WAL position of
`payload[0]`; the logical path ignores it, the physical walker checks
every message against its own position and declares desync on any gap.
The reverse direction is one frame, the standby status update: `d`,
`r`, then `!QQQQB` (the same LSN written three times, a timestamp in
microseconds since 2000-01-01, and a reply-requested flag). `c`, `C`
or `Z` mid-stream means the server ended replication; `E` carries an
ErrorResponse and raises `PgServerError` with the field map preserved.

## Logical path: the server decodes, pgnudge reads text

With logical decoding the output plugin has already turned WAL into
messages, so `wal.py` only extracts table names. There is no
intermediate dataclass on this path; a payload parses straight to zero
or more strings.

- **wal2json, format version 2** (one JSON object per change,
  `include-transaction=false`): parse the object, and if `action` is
  `I`, `U`, `D` or `T`, emit `schema.table`. Anything else, including
  unparsable payloads, is not a change and parses to `[]`.
- **test_decoding** (plain text): one regex,
  `^table (.+?): (?:INSERT|UPDATE|DELETE|TRUNCATE)`. A TRUNCATE line
  lists every affected table `", "`-joined, so the match can yield
  several names.

## Physical path: pgnudge decodes WAL itself

`XLogWalker` (`xlog.py`) is a sans-io state machine: feed it stream
chunks, get `RelChange | TxnEnd` events back. It parses record headers
and block references only; row payloads, full-page images and CRCs are
skipped by length, never decoded. Little-endian layouts, stable since
PostgreSQL 9.5.

### Page framing

WAL is a sequence of 8192-byte (`BLCKSZ`) pages, each opening with a
header the walker validates before anything else:

| field | format | check |
|---|---|---|
| xlp_magic | `<H` | high byte must be `0xD1`; known values 0xD113/0xD116/0xD118 (PG 16/17/18), an unknown one warns once and decoding continues |
| xlp_info | `<H` | `XLP_FIRST_IS_CONTRECORD` (0x0001) must agree with whether a record is in flight |
| xlp_tli | `<I` | skipped |
| xlp_pageaddr | `<Q` | must equal the stream position, else `WalSyncError` |
| xlp_rem_len | `<I` | tail of a record continuing from the previous page |

That is the 24-byte short header. The first page of each 16 MB segment
carries the 40-byte long form instead, appending `<QII` system id
(skipped), segment size (adopted) and block size (must be 8192).

Records are 8-byte aligned and interleaved with these headers, so a
record spanning pages is reassembled across them; its total length
lives in its first 4 bytes (`<I`, plausible range 24 bytes to 1 GiB),
which may themselves straddle a page boundary. Two skips make
arbitrary entry and segment switches work: the first page's
`xlp_rem_len` crosses a record already in flight at the start position
(which is why any LSN rounded down to its 8KB page boundary is a valid
entry point),
and an `XLOG_SWITCH` record zero-fills the rest of its segment with no
page headers at all, crossed by a raw byte count. Completed records
whose end position is at or before `emit_from` (the from-connect
watermark) are dropped: history, not news.

### Record header and dispatch

Every record starts with a 24-byte header; two fields decide
everything. The walker unpacks `<IIQBB` (xl_tot_len, xl_xid, xl_prev,
xl_info, xl_rmid), masks `op = info & 0x70`, and dispatches:

| rmid | op | event |
|---|---|---|
| 10 heap | 0x00 / 0x10 / 0x20 / 0x40 | `RelChange` insert / delete / update / hot_update |
| 9 heap2 | 0x50 | `RelChange` multi_insert (COPY) |
| 1 xact | 0x00 / 0x20 | `TxnEnd` commit / abort, with subxids |
| 1 xact | 0x10 | `TxnEnd` prepare, released as committed |
| 0 xlog | 0x40 | XLOG_SWITCH: arm the zero-fill skip |
| anything else | | skipped by length |

### Block references

After the record header come block-reference headers, then all payload
data. Every header declares its payload length up front, so walking
the headers alone locates the relation and the main-data offset
without touching row contents. One byte of block id selects the form:

| block id | content |
|---|---|
| 255 DATA_SHORT | main-data length, 1 byte |
| 254 DATA_LONG | main-data length, `<I` |
| 253 ORIGIN | 2 bytes, skipped |
| 252 TOPLEVEL_XID | 4 bytes, skipped |
| 0–32 | one block reference (below) |

A block reference is fork_flags (1 byte) plus data length (`<H`). Flag
0x10 (`BKPBLOCK_HAS_IMAGE`) appends a full-page-image header, `<HH`
image length and hole offset plus one bimg_info byte, and two more
bytes of compression header when the image is both holed (0x01) and
compressed (mask 0x1C); the image itself counts toward the data area.
Flag 0x80 (`BKPBLOCK_SAME_REL`) reuses the previous reference's
relation, otherwise `<III` gives the `RelFileLocator` (tablespace oid,
database oid, relnumber), followed by a 4-byte block number. The first
main-fork reference (`fork_flags & 0x0F == 0`) names the changed
relation. The walk ends when the remaining bytes equal the declared
data total; any mismatch is a `WalSyncError`, never a guess. Block
references are numbered 0 through 32 (`XLR_MAX_BLOCK_ID`); an id above
that, below the 252–255 marker range, is a desync. None of the record
types the walker cares about carries more than a few references, so the
high end is never exercised in practice.

### Transaction records

Commit and abort records carry their outcome in the main data, reached
via the block-reference walk's main-data offset. The layout is
sectional: 8 bytes of xact_time, then, only when xl_info has
`XACT_HAS_INFO` (0x80), an `<I` xinfo flag word whose bits append
sections in fixed order. The walker skips the 8-byte dbinfo section
(`XINFO_HAS_SUBXACTS` is 0x02, `XINFO_HAS_DBINFO` 0x01) to reach the
subxid array: `<i` count, then `<{count}I` transaction ids. A commit
releases the top-level xid and every listed subxid together. PREPARE
is released as committed at prepare time; the rollback-prepared trade
is covered in [physical-wal.md](physical-wal.md#commit-gating).

### The structures

| structure | fields | role |
|---|---|---|
| `RelChange` | xid, db_oid, relfilenode, kind | one heap change, pre-commit |
| `TxnEnd` | xid, committed, subxids | transaction outcome |
| `RelFileLocator` | spc_oid, db_oid, relnumber | relation identity as block refs carry it |
| `HeaderWalk` | locator, main_off | result of one header walk |
| `CommitGate` | pending: xid → {(db_oid, relfilenode): RelChange} | buffer until outcome known |
| `RelResolver` | names: relfilenode → str, cached | catalog lookup to `schema.table` |

`CommitGate.push` buckets each `RelChange` by xid, deduplicating on
(db_oid, relfilenode) within the transaction, and releases the bucket
on commit, drops it on abort. Past `max_open` (4096) open
transactions, the oldest is evicted *as if committed*: a spurious
nudge beats a lost one. `RelResolver` queries only unseen
relfilenodes; system-schema relations cache as drops (empty string),
and an unresolvable relfilenode is dropped uncached so its next
appearance retries, covering catalog visibility that lags the WAL by
one commit.

## Convergence: everything becomes a string

Both transports end at the same call: `_push_raw("schema.table")`.
From there the shared engine takes over:
`Intake` (a bounded queue of `Wakeup(payload, at)`; overflow sets a
flag instead of blocking, and the next window becomes
`Resync("overflow")`), `Coalescer` (one `Event` per payload, counting
arrivals), and `Debouncer` (a rolling window with an optional hard
cap) assemble the consumer contract from `core.py`:

- `Event`: `payload`, `first_seen`, `count`
- `Batch`: `events`, deduplicated, in arrival order
- `Resync`: `reason` is `connected | reconnected | overflow | failsafe`

The physical path's extra machinery (walker, gate, resolver) exists
only to reach parity with what the logical plugin hands over for free:
committed changes, named. Downstream of the string, the two transports
are indistinguishable.

## Sources

- PostgreSQL, [Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html):
  XLogData, Primary keepalive, Standby status update layouts;
  timestamps are microseconds since midnight 2000-01-01.
- PostgreSQL source (links pinned to REL_18_STABLE; layouts unchanged
  since the 9.5 [record-format revamp](https://github.com/postgres/postgres/commit/2c03216d831160bedd72d45f712601b6f7d03f1c)):
  [`access/xlogrecord.h`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/xlogrecord.h)
  (record header, block references, image and compression headers),
  [`access/xlog_internal.h`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/xlog_internal.h)
  (page headers, magics, segment layout),
  [`access/rmgrlist.h`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/rmgrlist.h)
  (resource-manager ids),
  [`access/heapam_xlog.h`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/heapam_xlog.h)
  (heap opcodes),
  [`access/xact.h`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/xact.h)
  (xinfo flags, subxid sections),
  [`catalog/pg_control.h`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/catalog/pg_control.h)
  (`XLOG_SWITCH`).
- [wal2json](https://github.com/eulerto/wal2json): format version 2,
  `include-transaction`. PostgreSQL,
  [test_decoding](https://www.postgresql.org/docs/current/test-decoding.html):
  output format; the `", "` join for multi-table TRUNCATE is
  [`pg_decode_truncate`](https://github.com/postgres/postgres/blob/REL_18_STABLE/contrib/test_decoding/test_decoding.c).
- The struct offsets above are verified live: the oracle test decodes
  a real WAL range with both this walker and `pg_waldump`, and the two
  must match exactly, on every PostgreSQL major in CI.
