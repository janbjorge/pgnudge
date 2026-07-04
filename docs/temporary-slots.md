# Logical decoding & `TEMPORARY` replication slots

pgwake turns PostgreSQL's write-ahead log into wakeups. The mechanism is
logical decoding over the streaming-replication protocol; the primitive that
makes it zero-footprint is the **temporary replication slot**. This page lays
out the machinery in PostgreSQL's own terms, the guarantees that fall out of
it — and the cases where you should use something else.

## The WAL is already a change feed

Every committed change is written exactly once regardless of who is
listening: to the write-ahead log. Logical decoding is the server facility
that re-reads WAL and hands each transaction, in commit order, to an *output
plugin* which formats it into messages.[^1] Nothing extra runs at write time —
no trigger fires, no row is queued, no table grows. The decoding cost lives on
the replication connection, not inside your transactions.

That is the whole trick: the change feed already exists. pgwake just asks the
server to replay it, live, in a throwaway session.

## Replication slots — and why the normal kind is a liability

A replication slot is the server's bookmark for one consumer: it pins the WAL
(and catalog rows) that consumer still needs. The manual is blunt about the
cost:

> Replication slots persist across crashes and know nothing about the state
> of their consumer(s). They will prevent removal of required resources even
> when there is no connection using them.[^2]

That persistence is the point for a replica — and exactly wrong for a nudge
feed. A crashed consumer that never comes back leaves a slot pinning WAL
forever; the disk fills; someone gets paged. Every CDC deployment carries this
risk as an operational duty ("if a slot is no longer required it should be
dropped"[^2] — by *you*).

## The temporary slot inverts the deal

The replication protocol offers one variant with the opposite lifetime:

> `TEMPORARY` — Specify that this replication slot is a temporary one.
> Temporary slots are not saved to disk and are automatically dropped on
> error or when the session has finished.[^3]

Slot lifetime **is** session lifetime, enforced by the server. Clean close,
crash, `kill -9`, yanked cable, `pg_terminate_backend` — same cleanup path,
no cleanup code. A disconnected pgwake consumer holds *nothing*: no WAL
retention, no catalog object, no disk.

The trade is that no bookmark survives disconnection, so there is no resume
and no replay. pgwake treats that as the design, not the bug: every
(re)connect creates a fresh slot and emits `Resync` — the consumer refetches
its data, and the database it refetches from is the source of truth.

## The wire conversation

One walsender session per feed, speaking the streaming-replication
protocol[^3] directly (stdlib asyncio + scramp; no driver):

```
startup    replication=database            (TLS, SCRAM-SHA-256)
Q          CREATE_REPLICATION_SLOT "pgwake_<pid>_<hex>"
             TEMPORARY LOGICAL wal2json (SNAPSHOT 'nothing')
Q          START_REPLICATION SLOT "…" LOGICAL 0/0 (…plugin options…)
W          CopyBothResponse                — stream is live
d/w        XLogData    one decoded change  → push "schema.table"
d/k        keepalive                       → standby-status reply
(socket dies, either end)                  → server drops the slot
```

Two details carry weight:

- **`SNAPSHOT 'nothing'`** skips snapshot export: the slot decodes strictly
  forward from its creation point.[^3] Pre-connect history is unreachable by
  mechanism, not by policy.
- **Slot creation waits** for write transactions in flight at connect time to
  finish. A long-running write delays connect — it never causes old changes
  to be delivered.

## Why the handshake is gap-free

`Resync` is emitted only *after* `CopyBothResponse` — after the stream is
live. Order of events:

```
t0  slot created            decoding start point fixed
t1  stream live             every commit > t0 will arrive
t2  Resync delivered        consumer refetches
t3  refetch executes        observes DB state ≥ t0
```

Any commit before t0 is included in the refetch. Any commit after t0 produces
a wakeup. Commits between t0 and t3 are covered *twice* — refetched and
nudged — which at-least-once semantics absorb for free, since refetching is
idempotent. There is no window in which a change is neither in the refetch
nor on the stream.

The same argument runs on every reconnect. Missed changes while disconnected
are bracketed by the reconnect `Resync`; the feed never pretends otherwise.

## Coalescing: wakeups, not rows

Payloads are identities (`schema.table`), not data, so identical payloads
inside one debounce window collapse into a single `Event` with a `count`. A
500-row transaction on one table is one wakeup and one refetch. Backpressure
degrades in the same direction: if the consumer falls behind and the intake
buffer overflows, pgwake drops the buffered hints and emits
`Resync("overflow")` — coarser, never wrong.

## When you should NOT use pgwake

pgwake trades completeness for zero footprint. When completeness is the
requirement, take the other side of the trade:

- **Every change must be processed.** pgwake misses events while disconnected
  *by design*. If missing one is unacceptable, you need the persistent slot
  and its operational duties — that is CDC (Debezium, or a hand-rolled
  persistent-slot consumer), not pgwake.
- **You need the row data.** Payloads carry identity only, no before/after
  images. Building an audit trail, replicating to another store, computing
  diffs — CDC.
- **A message must be processed exactly once, with retries and competing
  consumers.** That is a job queue. Use
  [pgqueuer](https://github.com/janbjorge/pgqueuer) — pgwake is its
  broadcast-shaped sibling, and broadcast has no delivery ledger.
- **Many consumers, directly attached.** Each feed holds one walsender + one
  slot against `max_wal_senders` / `max_replication_slots`, and each decodes
  the *whole database's* WAL (table filtering happens post-decode, in the
  plugin). Tens of direct feeds on a busy database multiply decode CPU. Run
  one feed in a bridge daemon and fan out over `NOTIFY` instead.
- **You can't get the server config.** Logical decoding needs
  `wal_level = logical` (restart), a role with `REPLICATION`, and a direct
  connection — replication traffic bypasses connection poolers such as
  PgBouncer. If any of those are off the table, so is pgwake.
- **Sub-debounce latency or strict ordering.** Wakeups are debounced,
  coalesced hints in arrival order — not an ordered event log. If consumers
  must observe individual changes in commit order, decode the stream
  yourself or use CDC.

## Operational limits

- One connected feed = one slot + one WAL sender. Disconnected = zero of
  everything, including retained WAL.
- `status_interval` (default 10 s) must stay under the server's
  `wal_sender_timeout` (default 60 s) or the server will kill the session as
  unresponsive.
- Grant `REPLICATION` to a dedicated role: logical decoding sees every
  table in the database, regardless of the payload filter.

## Sources

[^1]: PostgreSQL, [Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html):
    decoding model, output plugins, commit-order delivery.
[^2]: PostgreSQL, [Replication Slots](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS):
    slot persistence, resource pinning, the drop-it-yourself duty.
[^3]: PostgreSQL, [Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html):
    `CREATE_REPLICATION_SLOT` (`TEMPORARY`, `SNAPSHOT 'nothing'`),
    `START_REPLICATION`, CopyBoth message flow, standby status updates.
