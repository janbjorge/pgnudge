# Physical WAL, decoded client-side

How `RawFeed` turns a raw physical replication stream into
`schema.table` nudges with zero server footprint, and where the edges
are. Companion to [temporary-slots.md](temporary-slots.md), which covers
the logical transport; byte layouts and parser structures are in
[parsing.md](parsing.md).

## Why this transport exists

Logical decoding requires `wal_level=logical`, a cluster-wide setting
that usually needs a restart, and on managed platforms sometimes a
change request too. Physical replication works at `wal_level=replica`,
the stock default of every PostgreSQL since 9.6. `START_REPLICATION
PHYSICAL` also works **without a slot**: the client asks for a start
position and the server streams bytes. Nothing is created server-side
at any point, which is stronger than the logical transport's temporary
slot (an object, if a self-destructing one).

The trade is that physical WAL is a byte stream of internal record
structures, not decoded rows, and it covers the whole cluster. pgnudge
only needs "which relation changed", which turns out to require parsing
record *headers* only. Everything below the headers is skipped.

## What is parsed, and what never is

A WAL record is a 24-byte header (length, transaction id, resource
manager id, operation info) followed by block references (which
relation and block the record touches) followed by payload data (the
tuple bytes themselves). The walker in `xlog.py` reads:

- the header: 2 bytes of it decide everything (`xl_rmid`, `xl_info`);
- the block references: 12 bytes of RelFileLocator give tablespace,
  database, and relfilenode;
- xact records' subxid arrays, so commits release buffered changes.

Payload data, full-page images, and CRCs are never decoded; their
lengths are read only to skip over them. Row contents and column
values never leave the parser as anything but a byte count. These
header layouts have been stable since PostgreSQL 9.5; a new major's
page magic logs one warning and decoding continues (the oracle test
catches real breakage, see below). Little-endian servers only, which
is every mainstream platform.

Records that produce a nudge: heap insert, update, hot update, delete,
and heap2 multi-insert (COPY). Everything else, vacuum, pruning,
freezing, checkpoints, index churn, full-page writes, is filtered out
by resource manager and operation before any further work.

## Start position: no fast-forward, no history

`IDENTIFY_SYSTEM` returns the server's current position and timeline.
The feed:

1. queries `pg_current_wal_insert_lsn()` as the from-connect watermark
   (with asynchronous commit, flushed < inserted, and pre-connect
   writes may still be in WAL buffers; the insert position is the same
   point a logical slot would reserve);
2. starts streaming from the enclosing 8KB page boundary, because
   record framing is only recoverable at page starts (the page header's
   `xlp_rem_len` says how much of an in-flight record to skip);
3. suppresses events from records that end at or before the watermark.

Slot-less means the server retains nothing for a disconnected feed.
There is nothing to fast-forward through and nothing to resume;
reconnect takes a fresh "now" and emits `Resync`, exactly the
resync-not-resume model the feed contract already demands.

## Commit gating

Physical WAL carries changes in write order, before the transaction's
fate is known. Delivering them immediately would nudge on writes that
later roll back, and worse, a consumer's refetch could race an open
transaction and see nothing. The walker therefore tags each change with
its transaction id and a `CommitGate` buffers per transaction:

- commit record: release the transaction's changes (including its
  subtransactions', which the commit record lists);
- abort record: drop them;
- `PREPARE` (two-phase): release at prepare time; a later
  `ROLLBACK PREPARED` yields one spurious nudge, which at-least-once
  absorbs;
- too many open transactions (default cap 4096): evict the oldest as
  if committed, because a spurious nudge beats a lost one.

## Names

WAL identifies relations by (tablespace, database, relfilenode), not by
name; names live in the catalog. `RawFeed` keeps a second, plain
connection (walsender database mode accepts ordinary SQL, so it is
still driver-free) and resolves unseen relfilenodes against `pg_class`,
cached. `VACUUM FULL` and friends assign a new relfilenode; the next
write misses the cache and resolves fresh. A relfilenode that does not
resolve (a table created and written in a not-yet-visible transaction)
is dropped uncached and retries on its next appearance. System schemas
(`pg_catalog`, `pg_toast`, `information_schema`) resolve to a cached
drop, so catalog and TOAST churn never nudges. Other databases never
reach the resolver at all: the stream carries the whole cluster's WAL,
but any change whose database oid is not the connected database's is
dropped at commit time, before name resolution.

## The gaps, stated plainly

- **TRUNCATE does not nudge.** At `wal_level=replica` the WAL carries
  no reliable TRUNCATE signature (the record type it uses is shared
  with vacuum tail-truncation). The next write to the table nudges
  normally. Use `WalFeed` if TRUNCATE visibility matters.
- **Cluster-wide bandwidth.** The server streams all databases' WAL,
  plus index and maintenance traffic; filtering happens client-side.
  On a write-heavy cluster this is real network cost. Measure before
  running many `RawFeed`s; prefer the bridge daemon shape.
- **pg_hba.conf.** Physical replication connections match the
  `replication` pseudo-database, so `host all` rules do not admit them.
  One `host replication <role> ...` line is required.
- **Privileges.** `START_REPLICATION PHYSICAL` needs a role with the
  `REPLICATION` attribute, same as the logical transport. The catalog
  connection reads `pg_class` and `pg_namespace`, which any role can by
  default; only a locked-down catalog needs an extra grant.
- **Unlogged and temporary tables** write no WAL and therefore never
  nudge, on either transport.

## Tested how

Unit tests drive the walker over synthetic WAL built by a miniature
writer (page boundaries, split headers, continuation flags, alignment,
segment switches) and drive `RawFeed` against a scripted in-process
walsender. The live suite proves the contract on real servers,
including an untouched `wal_level=replica` container. The decoder
itself answers to an oracle: a live mixed workload's WAL range is
decoded by our walker and by `pg_waldump`, and the two change sequences
must match exactly, on every PostgreSQL major in CI.
