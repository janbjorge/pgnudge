# Parsing, walked end to end

A study companion to [parsing.md](parsing.md). Where that page is the
byte-level reference — every field, every offset — this one is the
*guided tour*: follow one row change from the wire to the string a
consumer sees, one step at a time, with the code you can open beside
each step.

Read this first to build the mental model, then use `parsing.md` as the
lookup table.

## The one job

pgnudge never carries your data. Its whole parsing stack exists to turn
replication traffic into one kind of fact:

> transaction *T* committed, and it touched table `schema.table`.

Everything below is in service of producing the string `"schema.table"`
and handing it downstream. If you keep that goal in mind, every parser
decision has an obvious reason: *does this byte help me name a committed
table?* If not, it is skipped.

## Two roads to the same string

There are two transports, and the only real difference is **who decodes
the WAL**.

```
                                       ┌────────────────────────────┐
  logical (WalFeed)   server decodes → │ "public.orders" text/JSON  │→ parse → "public.orders"
                                       └────────────────────────────┘
                                       ┌────────────────────────────┐
  physical (RawFeed)  raw WAL bytes  → │ 24B headers, block refs... │→ decode → "public.orders"
                                       └────────────────────────────┘
                                                                        ↓ both
                                        _push_raw("public.orders") → engine → Batch
```

- **Logical** (`wal.py`): PostgreSQL's output plugin already turned WAL
  into a short message that names the table. pgnudge just reads it.
  Easy.
- **Physical** (`xlog.py` + `raw.py`): the server ships raw WAL bytes.
  pgnudge decodes them itself — but only far enough to name the table,
  never the row. Harder, and where most of the code lives.

Downstream of the string, the two are indistinguishable.

## Code map (open these while you read)

| step | file · symbol |
|---|---|
| wire frames | `proto.py` · `WalsenderConnection.read_stream` |
| logical parse | `wal.py` · `_parse_wal2json_v2` / `_parse_test_decoding` |
| physical page/record framing | `xlog.py` · `XLogWalker.records`, `take_page_header` |
| physical record dispatch | `xlog.py` · `XLogWalker.parse_record` |
| block-reference walk | `xlog.py` · `XLogWalker.walk_headers` |
| commit buffering | `xlog.py` · `CommitGate.push` |
| relfilenode → name | `raw.py` · `RelResolver.resolve` |
| coalescing → contract | `engine.py` · `Intake`, `Coalescer`, `Debouncer` |
| the two item types | `core.py` · `Event`, `Batch`, `Resync` |

---

## Road 1 (logical): the easy walk

Say a consumer runs `WalFeed` with wal2json. You `INSERT` one row into
`public.orders`. The server's plugin emits one JSON object; it arrives
as the `payload` bytes of an `XLogData` frame:

```json
{"action":"I","schema":"public","table":"orders","columns":[]}
```

`_parse_wal2json_v2` does exactly one useful thing:

1. `json.loads(payload)` — unparsable? return `[]` (not a change).
2. Is `action` one of `I`, `U`, `D`, `T`? If yes, return
   `["public.orders"]`. If no (e.g. a `BEGIN`/`COMMIT`/message), `[]`.

That is the entire logical decode. `test_decoding` is the same idea with
a regex instead of JSON (`^table (.+?): (?:INSERT|UPDATE|DELETE|TRUNCATE)`),
and because a `TRUNCATE` line names several tables comma-joined, that one
can return more than one string.

Each returned string goes straight to `_push_raw(...)`. Jump to
[Convergence](#convergence-the-shared-tail).

---

## Road 2 (physical): the long walk

Same `INSERT`, but now with `RawFeed`. No plugin decoded anything — the
walker has to. This is the part worth studying.

### Step 0 — bytes arrive

`read_stream()` returns an `XLogData(start_lsn, end_lsn, payload)`. The
supervisor first checks continuity:

```python
expected = walker.pos + len(walker.buf)   # where the next byte must sit
if msg.start_lsn != expected:             # any gap = we lost the thread
    raise WalSyncError(...)               # abort, reconnect, resync
```

Then `walker.feed(msg.payload)` appends the bytes and drains whatever
completes. The walker is **sans-io**: it never touches a socket. You
feed it bytes; it yields events. That is why it can be tested against
synthetic WAL with no PostgreSQL (see `tests/test_xlog.py`).

The walker holds a little state machine:

| field | meaning |
|---|---|
| `pos` | absolute WAL position of `buf[0]` |
| `buf` | bytes received, not yet consumed |
| `skip` | bytes to discard (alignment padding, or an in-flight record's tail on the first page) |
| `raw_skip` | bytes to discard with **no** page headers inside (XLOG_SWITCH zero-fill) |
| `rec` / `rec_need` | the record being reassembled, and its total length once known |

### Step 1 — page framing

WAL is a stream of 8192-byte pages, each starting with a header. When
`pos` sits on a page boundary, the walker consumes the header *first*
and uses it as a self-check (`take_page_header`):

- **magic** — high byte must be `0xD1`; a fully unknown magic (new PG
  major) warns once and keeps going.
- **pageaddr must equal `pos`** — if the page says it lives at a
  different LSN than where we think we are, we have desynced. Raise.
- **continuation flag must agree** with whether a record is mid-flight.

The first page you ever see is special: its `xlp_rem_len` tells you how
many bytes belong to a record that was already in progress when you
attached. The walker sets `skip = rem_len` and steps over that tail.
*This is the trick that lets you start at any page boundary* — you don't
need to land exactly on a record start, you round your start LSN down to
the page (`page_floor`) and let `rem_len` carry you to the next whole
record.

### Step 2 — reassemble one record

Off a page boundary, the walker collects record bytes:

1. Cross alignment padding (records are 8-byte aligned) via `skip`.
2. Read the first 4 bytes → `tot_len` (`<I`). Sanity-bound it: at least
   the 24-byte header, at most 1 GiB, else `WalSyncError`.
3. Keep appending until `len(rec) == tot_len`. A record can span pages;
   when `pos` hits the next boundary mid-record, Step 1 runs again and
   the header is skipped *inside* the record's byte run.

When the record is whole, and its end is past `emit_from` (the
from-connect watermark — anything at or before it is history, not news),
it is yielded to `parse_record`.

### Step 3 — what kind of record is this?

`parse_record` unpacks the 24-byte header and looks at just two fields:
the resource manager (`rmid`) and the masked op (`info & 0x70`).

```
rmid=10 (heap),  op=0x00 → RelChange(kind="insert")   ← our INSERT lands here
rmid=10 (heap),  op=0x10/0x20/0x40 → delete/update/hot_update
rmid=9  (heap2), op=0x50 → RelChange(kind="multi_insert")
rmid=1  (xact),  op=0x00/0x20 → TxnEnd(commit/abort)
rmid=1  (xact),  op=0x10 → TxnEnd(prepare, treated as commit)
rmid=0  (xlog),  op=0x40 → XLOG_SWITCH: arm raw_skip to segment end
everything else → None (skipped)
```

Our insert is heap + 0x00, so we call `parse_heap(...)`, which needs the
*relation*. That comes from the block-reference walk.

### Step 4 — walk the block references to find the relation

After the 24-byte header, a record is laid out as **all the headers
first, then all the payload data**. Each header declares how many bytes
of payload it owns. So the walker can walk *only the headers*, summing
declared data lengths, and never read a single row byte. `walk_headers`
does this. One byte of "block id" selects the shape:

```
255 → main-data length follows (1 byte)     "here comes N bytes of main data"
254 → main-data length follows (<I)         (the long form)
253 → replication origin (2 bytes)          skip
252 → top-level xid (4 bytes)               skip
0..32 → an actual block reference           ← the relation lives here
```

For our insert, a block reference (id 0, main fork). Its shape:

| field | bytes | our example | note |
|---|---|---|---|
| fork_flags | 1 | `0x20` | HAS_DATA set; not an image, not SAME_REL |
| data_len | `<H` | `12` | length of this block's data payload |
| *(if HAS_IMAGE 0x10)* | var | — | full-page-image header, skipped by length |
| RelFileLocator | `<III` | `(1663, 5, 16391)` | tablespace, **db oid**, **relfilenode** |
| block number | `<I` | `0` | which page — irrelevant to us, skipped |

The first main-fork reference (`fork_flags & 0x0F == 0`) wins: its
locator names the changed relation. The walk ends when remaining bytes
equal the summed data total — a mismatch is a `WalSyncError`, never a
guess. (Two flags add optional sections mid-reference: `0x10` a
full-page image, `0x80` "same relation as the previous reference"; both
are handled by length, see `parsing.md` for the exact bytes.)

Out comes:

```python
RelChange(xid=742, db_oid=5, relfilenode=16391, kind="insert")
```

Note what we have and don't: a **relfilenode** (a physical file number),
not a table name. Naming it is Step 6. First, is the transaction even
real?

### Step 5 — the commit gate (don't nudge on a rollback)

Physical WAL records changes *as they are written*, before anyone knows
if the transaction commits. If we nudged now and the transaction rolled
back, a consumer would refetch for nothing — worse, it could refetch
mid-transaction. So `CommitGate` buffers:

```
gate.push([RelChange(xid=742, ...)])        → []        # held, not released
...more records for xid 742...
gate.push([TxnEnd(xid=742, committed=True)]) → [RelChange(...)]  # released now
```

Inside the gate, `pending` is `xid → {(db_oid, relfilenode): RelChange}`.
Two consequences fall out of that shape:

- **Dedup within a transaction.** Insert + update + delete of the same
  table in one txn collapse to one `RelChange` (first wins) — a nudge is
  "this table moved", not a change count.
- **Bounded memory.** Past `max_open` (4096) open transactions, the
  oldest is evicted *as if it committed*. A spurious nudge beats a lost
  one. A rolled-back transaction's bucket is simply dropped, never
  released.

A `COMMIT` for xid 742 (and any listed subxids) releases the bucket.
Now, finally, a name.

### Step 6 — relfilenode → `schema.table`

`RelResolver` turns physical file numbers into names via a catalog
query, and it is lazy and self-healing:

- Only **unseen** relfilenodes are looked up (one `pg_class` join),
  then cached — event-driven, never polled.
- Relations in system schemas (`pg_catalog`, `pg_toast`,
  `information_schema`) cache as an empty string — a permanent "ignore".
- A relfilenode that *doesn't resolve* is dropped **uncached**, so its
  next appearance retries. This matters: catalog visibility can lag the
  WAL by one commit (you can see the change before you can see the new
  name), and the retry covers exactly that window.

`push_committed` also drops changes from other databases (physical WAL
is cluster-wide; a `RawFeed` only cares about its own `db_oid`) and
applies the client-side `tables` filter. What survives:

```python
self._push_raw("public.orders")
```

Same call the logical road reached in one step.

---

## Convergence: the shared tail

Both roads now hold a plain string and call `_push_raw`. The engine
(`engine.py`) assembles the consumer contract from here, and it is
identical for both transports:

1. **`Intake`** — a bounded queue of `Wakeup(payload, at)`. If it fills
   (a consumer too slow, or a storm), it does **not** block the stream;
   it sets an overflow flag, and the next window becomes
   `Resync("overflow")` — "I gave up counting, reload everything".
2. **`Coalescer`** — one `Event` per distinct payload, counting arrivals.
   50 inserts into `public.orders` in one window → **one**
   `Event(payload="public.orders", count=50)`.
3. **`Debouncer`** — a rolling quiet window (`debounce`), hard-capped by
   `max_batch_wait`, then flush a `Batch`.

The consumer sees only two item types (`core.py`):

- **`Batch`** — a window of `Event`s, deduplicated, in arrival order.
  "These tables moved; go refetch them."
- **`Resync`** — "reload everything." Reason is `connected`,
  `reconnected`, `overflow`, or `failsafe`. Every gap (a reconnect, an
  overflow) is bracketed by one of these, which is the whole reliability
  model: pgnudge never promises it saw every change, it promises every
  gap is announced.

## Why the physical road bothers

Look back at the two roads. The physical path builds a walker, a commit
gate, and a resolver — pages of code — to arrive at the exact same
`_push_raw("public.orders")` the logical plugin handed over for free.
That is the point: the extra machinery exists **only to reach parity**.
Committed changes, named, nothing else. It buys you `RawFeed`'s one
advantage — it runs on a stock `wal_level=replica` server with *no*
server-side object at all — at the cost of doing the decode yourself.

## Watch it happen

The fastest way to cement this is to see it live. The CLI exposes the
whole pipeline at increasing verbosity:

```bash
pgnudge watch physical -vvv    # TRACE: every XLogData frame + decoded events
pgnudge watch logical  -vv     # DEBUG: each committed schema.table (the nudge)
```

At `-vvv` the `trace_frame` taps print each wire frame with its LSN
range and the events the walker produced from it — you can literally
watch Step 0 → Step 4 scroll by.

And the proof the byte offsets above are right: `tests/test_raw.py`'s
oracle test decodes a real WAL range with *both* this walker and the
server's own `pg_waldump`, and asserts they match exactly, on every
PostgreSQL major in CI. If a single offset in `walk_headers` were wrong,
that test would diverge.

## Read next

- [parsing.md](parsing.md) — the exhaustive byte-level reference for
  every field mentioned here.
- [physical-wal.md](physical-wal.md) — *why* the physical transport is
  shaped this way (commit gating, name resolution, the stated gaps).
- [temporary-slots.md](temporary-slots.md) — the logical transport and
  the gap-free handshake.
- The source, in the order this doc walked it: `proto.py` → `xlog.py` →
  `raw.py` → `engine.py` → `core.py`.
