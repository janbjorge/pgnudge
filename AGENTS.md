# AGENTS.md — Guidance for AI Code-Generation Agents

pgnudge is a Python framework for **push-only change nudges from PostgreSQL
with zero server footprint**. Consumers get an async iterator of two item
types — `Resync` (reload everything) and `Batch` (coalesced wakeups saying
*which tables moved*) — and refetch their own data. The framework never
carries application data. Python >= 3.13, async-first, MIT-licensed,
runtime dependency: scramp only.

Named `pgnudge` (v1.0.0): *pgqueuer moves work, pgnudge moves wakefulness.*

Read this file fully before changing anything. The full design history is
deliberately not in this repo (`docs/` holds only the public
temporary-slots reference); settled decisions are summarized in the five
laws below — ask the owner before re-opening any of them.

## The five laws (non-negotiable, owner-imposed)

These were each fought for across a long design process. Do not trade any of
them away, even for apparently good reasons. If a task seems to require
violating one, stop and ask the owner.

1. **Push only.** No polling of any kind — no `pg_logical_slot_get_changes`,
   no periodic SELECTs, no sentinel queries. The only timers permitted:
   reconnect backoff, standby-status feedback (a protocol keepalive on an
   open stream, not a data poll), and the *user-opt-in* `failsafe` knob
   (default off).
2. **Native Postgres only.** No external CDC systems, brokers, or sidecars
   beyond an optional bridge daemon that is itself just this library.
3. **Zero server persistence.** Nothing created on the server may outlive
   the connection. The only primitive in all of PostgreSQL that satisfies
   this for a change feed is the **temporary replication slot** ("not saved
   to disk, automatically dropped on error or when the session has
   finished"). Triggers were explicitly vetoed by the owner: persistent
   catalog objects fail this law by definition. `ddl.py` (a trigger
   generator) was the boxed exception; the owner had it deleted 2026-07-03.
   Do not reintroduce a trigger path.
4. **Driver-free.** The replication transport is a hand-rolled walsender
   client (`proto.py`, stdlib + scramp), because no Python library outside
   psycopg2 speaks the replication protocol (asyncpg issue #91 has been
   open since 2017) and psycopg2 is vetoed. Runtime dependency: scramp
   only. asyncpg exists solely in the dev group for the live test's admin
   connection.
5. **From-connect-only.** No history, no backfill, no replay. Slots are
   created fresh at every (re)connect with `SNAPSHOT 'nothing'`; reconnect
   *resyncs*, never resumes. Missing changes while disconnected is a
   requirement, not a bug — the `Resync` item brackets every gap and the
   consumer's refetch is the source of truth.

## Things you will be tempted to do — don't

- **Add a graceful `DROP_REPLICATION_SLOT` or protocol goodbye on close.**
  `WalFeed._extra_close()` hard-aborts the socket *on purpose*: crash and
  clean exit must exercise the identical server-side cleanup path, and the
  test suite depends on proving that. Cleanup is the server's job.
- **Persist the slot / store an LSN cursor to "not miss events".** Violates
  laws 3 and 5. Resync-plus-refetch *is* the reliability model.
- **Wrap protocol reads in `asyncio.wait_for`.** Cancelling a read between
  frame header and body desyncs the stream. Feedback runs on its own timer
  task precisely so reads can block freely. Keep it that way.
- **Put row data in payloads by default.** The v1 payload contract is
  `schema.table`, and coalescing depends on payload identity. A `:pk`
  payload is a planned *opt-in* (see Backlog).
- **Switch to pgoutput to drop the wal2json dependency.** Careful: pgoutput
  requires `publication_names`, and a **publication is a persistent catalog
  object** — it likely violates law 3. This roadmap item is conditional at
  best; raise it with the owner before starting (a pre-existing
  app-owned publication *might* be acceptable as "config", might not).
- **Reintroduce a trigger-based emitter.** `ddl.py` existed once and was
  deleted on owner instruction (2026-07-03). Persistent catalog objects
  violate law 3; fan-out goes through the bridge daemon.
- **Reintroduce a NOTIFY consumer.** There was a second transport,
  `ChangeFeed` (NOTIFY consumer on asyncpg); the owner had it removed
  2026-07-03. pgnudge is WalFeed-only. Fan-out is a bridge daemon
  republishing via `pg_notify`; consumers LISTEN with whatever driver they
  already have.

## Project Structure

```
src/pgnudge/
  proto.py    WalsenderConnection: the protocol client (~240 lines, stdlib
              asyncio + scramp only). Startup with replication=database,
              optional TLS via StreamWriter.start_tls, auth = trust /
              cleartext / SCRAM-SHA-256 (-PLUS mechanisms filtered out; no
              channel binding), simple_query (walsender mode speaks ONLY the
              simple query subprotocol), start_replication -> CopyBoth,
              read_stream() -> XLogData | Keepalive (frozen slots
              dataclasses), send_standby_status(), abort() = deliberate
              hard close.
  core.py     The contract only: Event/Batch/Resync dataclasses + FeedItem.
              Event is payload/first_seen/count — `channel` and
              `payload_filter` were dropped 2026-07-04 (breaking, owner
              call); do not reintroduce.
  engine.py   The machinery, one dataclass per concern, pure stdlib:
              Wakeup (one raw arrival, pre-coalescing),
              Intake (bounded wakeup buffer, overflow flag),
              Coalescer (dedup buffer, count per payload),
              Debouncer (rolling window, hard max_batch_wait cap; overflow
              -> Resync("overflow")), Backoff (jittered exponential),
              FeedService (wires intake -> debouncer -> out queue, owns
              tasks/failsafe/shutdown), BaseFeed (thin async-iterator
              facade; subclass hooks _supervisor/_extra_close). Unit-tested
              without PostgreSQL in tests/test_engine.py.
  wal.py      WalFeed(BaseFeed): the transport. Supervisor creates a fresh
              TEMPORARY slot per (re)connect (name: pgnudge_<pid>_<hex>),
              SNAPSHOT 'nothing', starts replication at 0/0, emits Resync
              only once the stream is live (this ordering is the gap-free
              handshake argument — see README), parses wal2json
              format-version 2 (default) or test_decoding (zero-install
              fallback), feedback task every status_interval (default 10s,
              must stay under wal_sender_timeout, default 60s) plus
              immediate reply on keepalive reply-requested.
tests/
  conftest.py  session-scoped PostgreSQL via testcontainers (pgqueuer
               pattern), scratch database per test, best-effort TLS enable
               (self-signed cert) inside the container. Env knobs:
               EXTERNAL_POSTGRES_DSN, POSTGRES_IMAGE, PGNUDGE_PLUGIN,
               PGNUDGE_TLS.
  test_engine.py  unit tests for the engine classes (Intake, Coalescer,
               Debouncer, Backoff, FeedService, BaseFeed via a FakeFeed) —
               pure asyncio, no PostgreSQL.
  wire.py      shared wire-protocol helpers for the fake-walsender tests:
               backend frame builders, frontend frame readers, a
               scripted_server() harness on an ephemeral localhost port.
  test_proto.py  WalsenderConnection unit tests against scripted handlers —
               no PostgreSQL. Auth edge cases (cleartext, missing password,
               MD5 rejected, SASL -PLUS-only rejected), SSL-refused, silent-
               server timeout, error/notice handling, read_stream frame
               parsing, standby-status wire format, abort idempotence.
  test_wal_unit.py  WalFeed unit tests — no PostgreSQL. wal2json/
               test_decoding payload parsers, plugin option assembly +
               SQL quoting, feedback loop, connect/slot-failure retry, and
               a full lifecycle against a scripted FakeWalsender (connect ->
               Resync -> Batch -> keepalive ack -> stream end -> reconnect),
               incl. asserting TEMPORARY + SNAPSHOT 'nothing' on the wire.
  test_wal.py  7 tests incl. the two proofs: no backfill of pre-connect
               writes, and hard abort -> pg_replication_slots EMPTY.
docs/
  temporary-slots.md  public reference (added 2026-07-04): logical decoding
               + temporary-slot mechanics in PostgreSQL's own terms, the
               gap-free handshake, polling vs pgnudge, when NOT to use
               pgnudge, operational limits. Reference material only — not
               a design-history document.
examples/
  minimal.py   smallest end-to-end consumer: WalFeed + match on Resync/Batch.
.github/workflows/
  ci.yml       lint (ruff + mypy), test matrix Python 3.13/3.14 × PG 16-18
               (test_decoding), one wal2json job on a CI-built image.
```

The consumer contract, coalescing semantics, and ops notes are in
`README.md` — treat it as the spec. The full decision history (why not
sentinel polling, why not triggers, why not Go/Rust/psycopg2, the Zig
track) lived in `docs/design-history.md` and `docs/research-notifications.md`,
removed from the repo 2026-07-03 (owner wanted no origin-story/internal
context public). `docs/temporary-slots.md` (2026-07-04) is NOT that history
returning — it is a public reference on the temp-slot mechanism itself.
Settled decisions are summarized in the five laws above; ask the owner
before re-litigating any of them.

## Build, Lint, and Test Commands

All commands use `uv` as the package manager.

```bash
uv sync                              # .venv + uv.lock, editable + dev group

# Run all tests (spins postgres:17 via testcontainers, runs the proofs)
uv run pytest

# Run a single test file
uv run pytest tests/test_engine.py

# Run a single test function
uv run pytest tests/test_wal.py::test_hard_abort_leaves_no_slots

# Coverage (100% line+branch as of 2026-07-04; keep it there.
# CI gate is 98 — headroom for best-effort TLS lines)
uv run pytest --cov=pgnudge --cov-report=term-missing

# Lint + typecheck (both must stay clean)
uv run ruff check . && uv run mypy

uv build
```

### Database for Tests

The test suite owns its own PostgreSQL via testcontainers — nothing to
install beyond Docker. Against an external server instead:
`EXTERNAL_POSTGRES_DSN=postgresql://... uv run pytest` (role needs
CREATEDB + REPLICATION; add `PGNUDGE_TLS=1` if it has TLS). Default plugin
is `test_decoding` (zero-install); `PGNUDGE_PLUGIN=wal2json` needs an image
that ships it — **no public image does for PG 16** (debezium/postgres
dropped wal2json; proven missing 2026-07-04, error 58P01). Build one:
`postgres:16` + apt `postgresql-16-wal2json` from the PGDG repo the
official image already has configured — see the docker build step in
`ci.yml`, then `POSTGRES_IMAGE=pgnudge-wal2json:16`.

Debugging trick from the PG docs: a replication connection can be tested
with nothing but psql —
`psql "dbname=x replication=database user=y" -c "IDENTIFY_SYSTEM;"`

## Code Style

### Formatting and Linting

- **Formatter/linter**: ruff (line-length=110, `src = ["src"]`)
- **Mypy**: `strict` + `disallow_any_explicit` + `warn_unreachable`, extra
  error codes `redundant-expr`, `possibly-undefined`, `truthy-bool`,
  `ignore-without-code`; covers `src` and `tests`.

### Imports

1. Standard library
2. Third-party packages (scramp; asyncpg/pytest/testcontainers in tests)
3. Internal imports using absolute paths (`from pgnudge.core import Batch`)

- **No `from __future__ import annotations`.** pgnudge targets Python >= 3.13;
  modern syntax is used directly (PEP 695 `type` aliases and generics,
  `Self`, `collections.abc` imports). This deliberately deviates from
  pgqueuer, which supports 3.10.
- **No local imports.** All imports at module top level; the only exception
  is `if TYPE_CHECKING:` blocks for breaking circular imports at runtime.

### Type Annotations

- **Always annotate** all function/method signatures, tests included.
- Use native types: `list[str]`, `int | None` (never `Optional`), PEP 695
  `type` aliases (`type FeedItem = Resync | Batch`), `ClassVar` for class
  constants, `Self` for fluent/classmethod returns.
- **No `Any`, anywhere.** `disallow_any_explicit` is on and there are no
  exceptions — not even at protocol boundaries. Use proper types, generics,
  protocols, or `object` instead. (Stricter than pgqueuer's
  driver-boundary carve-out; the stricter rule wins.)
- **No `# type: ignore` in production code** (`src/pgnudge/`). Fix the
  underlying type issue instead. In tests it is acceptable only with an
  error code (`ignore-without-code` is enforced).
- **Generic constructors over type-annotated assignments**:
  `fut = asyncio.Future[MyType]()` not
  `fut: asyncio.Future[MyType] = asyncio.Future()`.

### Naming Conventions

| Element             | Convention        | Example                             |
|---------------------|-------------------|-------------------------------------|
| Classes             | CamelCase         | `WalFeed`, `PgServerError`          |
| Functions/methods   | snake_case        | `read_stream`, `send_standby_status`|
| Class constants     | UPPER_SNAKE `ClassVar` | `_PG_EPOCH_UNIX`               |
| Test functions      | `test_` prefix    | `test_hard_abort_leaves_no_slots`   |
| Fixtures            | snake_case        | `pg`, `admin`, `postgres`           |

- **No module-level globals.** Constants live as class attributes
  (`ClassVar`); test config lives inside functions. (Owner rule 2026-07-03;
  stricter than pgqueuer's module-level constants — the stricter rule wins.)
- **No leading-underscore prefixes on new names.** Python has no real
  public/private distinction; pick a descriptive name instead of hiding it
  behind a prefix. (Adopted from pgqueuer 2026-07-04 as the stricter rule —
  it supersedes the earlier pgnudge carve-out that kept single underscores
  on plain-class instance attributes and helpers.) Existing `_`-prefixed
  internals in `proto.py`/`engine.py`/`wal.py` predate this; renaming them
  is a pending refactor — coordinate with the owner before a mass rename,
  and never underscore-prefix anything new. Module and class names and
  dataclass fields were already banned from underscore prefixes
  (internal dataclass state uses `field(init=False)` with a plain name).
  Python-mandated protocol dunders (`__init__.py`, `__aenter__`,
  `__version__`, …) are exempt.

### Error Handling

- The protocol layer raises `PgServerError` (ErrorResponse field map
  preserved) and stdlib connection errors; the supervisor catches, aborts,
  and reconnects with a fresh slot — errors are a normal part of the
  lifecycle, not exceptional control flow to hide.
- Use `contextlib.suppress(...)` for non-critical teardown errors.
- Use `pytest.raises` in tests for expected exceptions.

### Docstrings

Follow [PEP 257](https://peps.python.org/pep-0257/). Keep them tight;
design prose lives in README/docs, not code.

- **One-liner**: triple-quoted on the same line, period at the end.
- **Multi-line**: summary line, blank line, body. Closing `"""` on its own
  line.
- **Don't restate the signature.** Only mention a parameter when something
  non-obvious matters (units, constraints, ownership, side effects).
- **Skip docstrings on trivial helpers.** A descriptive name beats a
  docstring that repeats it.
- **Test docstrings**: one line describing the behavior under test. Skip
  when the test name is self-evident.
- **Module docstrings**: short; say what the module is, point to README for
  semantics.

### Comments

Default to no comments. Code with descriptive names is more durable than
comments that rot.

Only add a comment when the **why** is non-obvious:

- A hidden invariant or constraint that the code relies on (e.g. "hard-close
  on purpose, no DROP: crash and clean exit must exercise the same
  server-side cleanup path")
- A workaround for a specific bug, library quirk, or platform behavior
- A subtle ordering or concurrency requirement
- Something that would surprise a reader who understood the code

**Do not write:** comments that restate the code, caller references, task
or issue refs in inline comments (regression tests may reference an issue
in their docstring), banner section headers beyond the existing light
`# -- section --` dividers, or TODOs without an owner and a tracking link.

### Guiding Principles

- **Follow existing patterns.** Before writing new code, read surrounding
  modules to match conventions. Do not invent new patterns when an
  established one exists.
- **Readability and correctness above speed.** Never sacrifice clarity or
  correctness for performance unless there is a measured, proven need.
- **Every change must be proven correct by a test.** Never accept a code
  change without an accompanying test. Tests must be narrow and precise —
  test exactly the behavior being changed. Coverage is 100% line+branch;
  keep it there.
- **Every user-facing change must be documented.** `README.md` is the spec —
  new knobs, contract changes, and behavior changes update it in the same
  change. `docs/temporary-slots.md` covers the mechanism; extend it when
  the machinery changes.

## Testing Conventions

- **pytest config**: `asyncio_mode = "auto"`, function-scoped event loops.
- Declare async tests as `async def test_...` — no `@pytest.mark.asyncio`
  decorator needed.
- Annotate all test function parameters and returns
  (`async def test_foo(pg: PgParams) -> None:`).
- Fixtures create a **fresh scratch database per test**; the PostgreSQL
  container is session-scoped.
- Key fixtures in `tests/conftest.py`: `postgres` (session server),
  `pg` (per-test `PgParams`), `admin` (asyncpg connection into the scratch
  DB).
- Engine, proto, and WalFeed-unit tests need no PostgreSQL at all — the
  wire-level tests run against `tests/wire.py`'s scripted fake walsender.
  Prefer that harness for new protocol behavior; reserve `test_wal.py` for
  properties only a real server can prove.

## Versioning

pgnudge follows **strict semantic versioning** (SemVer) from v1.0.0 onward:

- **Patch** (1.0.x): bug fixes only, no API changes.
- **Minor** (1.x.0): new features, fully backward-compatible.
- **Major** (x.0.0): breaking changes — reserved for when there is no
  alternative.

The stable API is the feed contract (`Resync | Batch` iteration) and the
payload contract (`schema.table`) — transports are pluggable behind them
(this keeps the someday Zig track possible). Never break either in a patch
or minor release.

## Commit Conventions

This project follows
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

```
<type>[optional scope]: <description>
```

- **type**: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`,
  `build`, `ci`, `chore`
- **scope** (optional): area of the codebase, e.g. `proto`, `engine`,
  `wal`, `core`
- **description**: imperative mood, lowercase, no period at the end
- **`!` after type/scope** marks a breaking change (e.g.
  `refactor!: drop Event.channel ...`)
- Body: present tense, wrap at 72 characters. Explain **why**, not just
  what. Footer `BREAKING CHANGE: <description>` for breaking changes.
- No PR-number suffix — pgnudge has no PR-based flow today; if one appears,
  adopt pgqueuer's `(#{PR_ID})` suffix.

## CI Matrix

Python 3.13/3.14 × PostgreSQL 16–18 on Ubuntu (test_decoding), plus one
wal2json job on a CI-built `postgres:16` + PGDG `postgresql-16-wal2json`
image (the debezium/postgres:16 image does NOT ship wal2json — first CI
run failed on it 2026-07-04, fixed same day). Lint job runs ruff + mypy.
Coverage gate `--cov-fail-under=98`.

## Proven vs. untested (be honest in docs and commits)

Proven live (PG 16 with wal2json, PG 17 with test_decoding via the pytest
suite): SCRAM-SHA-256 auth; TLS handshake + SCRAM over TLS (self-signed
cert, CERT_NONE context); temporary slot is the only server object while
connected; no backfill; client-side coalescing (50-row txn -> one Event,
count=50); kill -> fresh slot, old auto-dropped; hard abort ->
`pg_replication_slots` empty; wheel installs and passes from site-packages
(pre-1.0 layout — re-verify from wheel before release).

Untested / absent: `ssl=True` verify-full against a real managed endpoint
(Azure Flexible Server is the owner's target — replication bypasses the
built-in PgBouncer there, use direct 5432); MD5 auth (deliberately absent);
pgoutput (see the publications warning above); behavior under a
long-running write transaction at connect (slot creation waits — connect
latency, never history); PG 18 (in the CI matrix, not yet run); the CI
workflow itself.

## Backlog, priority order

0. **Rename — DONE** (`pgnudge` → `pgnudge`, 2026-07-03, owner's choice).
   Runners-up preserved in case of a future collision, PyPI-verified free
   at the time: `waltail`, `pgripple`, `pgnudge`, `pgwisp`, `pgghost`,
   `pgfollow`, `pgblip`, `pgwhisper`, `pgpulse`, `pgbeacon`, `pgfeed`
   (`pgtail`, `pgflux`, `pglive` were taken). The PyPI name `pgnudge` is
   **not yet registered** — claiming it with the first release is the real
   Task 0 remainder.
1. ~~pytest-ify~~ — DONE 2026-07-03: testcontainers session fixture,
   scratch DB per test, EXTERNAL_POSTGRES_DSN escape hatch.
2. ~~CI~~ — DONE 2026-07-03: `.github/workflows/ci.yml`, Python 3.13/3.14 ×
   PG 16–18 (test_decoding; floor is PG 16+, owner call 2026-07-03) + one
   wal2json job on a CI-built image.
   Repo pushed to github.com/janbjorge/pgnudge 2026-07-03 — check Actions
   for the first real runs.
3. **Bridge daemon** as a first-class example or subpackage: one WalFeed ->
   `pg_notify` on a channel -> plain LISTEN consumers. Zero persistent
   objects end to end (the bridge's slot dies with the bridge).
4. **Opt-in payload v2** (`schema.table:pk`) — wal2json carries pk columns;
   keep v1 the default, document the coalescing trade (dedup granularity
   changes).
5. Azure end-to-end validation (verify-full TLS, direct 5432, wal2json
   preinstalled) — coordinate with owner, needs real infra.
6. ~~Typing/lint hardening~~ — DONE 2026-07-03: mypy --strict and ruff
   configured in pyproject, both clean. Keep them clean. Docs polish remains.

## Release checklist

Bump `__version__` and `pyproject.toml` together; `uv build`; run the full
suite from the installed wheel (not the source tree) against a live server;
tag; `uv publish`. Never release with the live suite skipped — the
zero-footprint proof (`test_hard_abort_leaves_no_slots`) is the product.

## Owner context

Python/asyncio shop; the owner maintains pgqueuer (job queue on
LISTEN/NOTIFY) — this library is its broadcast-shaped sibling: *pgqueuer
moves work, this moves wakefulness*. Target deployment: Azure Database for
PostgreSQL Flexible Server, PG 16+, schema `versaai`, read-only consumers.
A native Zig implementation of the walsender core is a someday-ambition
("the Zig track") — design decisions here should not foreclose it: the
feed contract and payload contract are the stable API, transports are
pluggable.
