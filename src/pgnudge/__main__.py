"""pgnudge CLI: open a feed and watch the parsed replication stream.

The default output is the product itself: one line per ``Batch`` or
``Resync``. Raise the verbosity to study the layers underneath.

Verbosity ladder (repeat ``-v``):

    (none)  WARNING  only the product: each Batch / Resync
    -v      INFO     + connection lifecycle (streaming, reconnect)
    -vv     DEBUG    + each committed schema.table, txn eviction
    -vvv    TRACE    + every wire frame and decoded record

Connection settings fall back to the libpq ``PG*`` environment variables
(PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD); explicit flags win.
"""

import argparse
import asyncio
import logging
import os

from pgnudge import Batch, RawFeed, Resync, WalFeed, __version__
from pgnudge.doctor import diagnose
from pgnudge.engine import TRACE, BaseFeed

_LEVELS = (logging.WARNING, logging.INFO, logging.DEBUG, TRACE)


def _verbosity_level(count: int) -> int:
    return _LEVELS[min(count, len(_LEVELS) - 1)]


def build_parser() -> argparse.ArgumentParser:
    connection = argparse.ArgumentParser(add_help=False)
    conn = connection.add_argument_group("connection")
    conn.add_argument("--host", default=os.environ.get("PGHOST", "127.0.0.1"))
    # a string default goes through type=int at parse time, so a bad PGPORT
    # becomes a clean parser error instead of a traceback
    conn.add_argument("--port", type=int, default=os.environ.get("PGPORT") or "5432")
    conn.add_argument("--user", default=os.environ.get("PGUSER"))
    conn.add_argument("--database", default=os.environ.get("PGDATABASE"))
    conn.add_argument("--password", default=os.environ.get("PGPASSWORD"))
    conn.add_argument(
        "--ssl",
        action="store_true",
        default=os.environ.get("PGSSLMODE") in ("require", "verify-ca", "verify-full"),
        help=(
            "require TLS. Turned on by PGSSLMODE=require/verify-ca/verify-full, all "
            "treated as verify-full (cert + hostname verified against system CAs); "
            "prefer/allow/disable/unset stay plaintext, unlike libpq's prefer which "
            "would try TLS first"
        ),
    )
    connection.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="repeat for more: -v lifecycle, -vv names, -vvv frames+records",
    )

    common = argparse.ArgumentParser(add_help=False, parents=[connection])
    common.add_argument(
        "--table",
        action="append",
        metavar="SCHEMA.TABLE",
        help="only watch this table; repeatable",
    )
    knobs = common.add_argument_group("feed")
    knobs.add_argument("--debounce", type=float, default=0.05, help="quiet window before a batch closes (s)")
    knobs.add_argument("--status-interval", type=float, default=10.0, dest="status_interval")
    knobs.add_argument("--failsafe", type=float, default=None, help="emit a periodic resync every N seconds")

    parser = argparse.ArgumentParser(
        prog="pgnudge",
        description="Watch the parsed pgnudge replication stream, or check server readiness.",
    )
    parser.add_argument("--version", action="version", version=f"pgnudge {__version__}")
    # Always present so main() can test it without a subcommand having run.
    parser.set_defaults(transport_kind=None)
    commands = parser.add_subparsers(dest="command", metavar="<command>")

    # Action-first so the top level stays open for more verbs; transport is
    # nested under watch, and doctor is a sibling that needs no transport.
    doctor = commands.add_parser(
        "doctor", parents=[connection], help="probe the server and recommend a transport"
    )
    doctor.add_argument(
        "--plugin",
        choices=("wal2json", "test_decoding"),
        default="wal2json",
        help="output plugin to probe for the WalFeed (logical) check",
    )

    watch = commands.add_parser("watch", help="stream and print the parsed replication feed")
    transports = watch.add_subparsers(dest="transport", metavar="{logical,physical}")
    logical = transports.add_parser(
        "logical",
        aliases=["wal"],
        parents=[common],
        help="logical decoding via a temporary slot (needs wal_level=logical); alias: wal",
    )
    logical.add_argument("--plugin", choices=("wal2json", "test_decoding"), default="wal2json")
    logical.set_defaults(transport_kind="logical")
    physical = transports.add_parser(
        "physical",
        aliases=["raw"],
        parents=[common],
        help="slot-less physical WAL, decoded client-side (works at wal_level=replica); alias: raw",
    )
    physical.set_defaults(transport_kind="physical")
    return parser


def _build_feed(args: argparse.Namespace) -> WalFeed | RawFeed:
    if args.transport_kind == "logical":
        return WalFeed(
            host=args.host,
            port=args.port,
            user=args.user,
            database=args.database,
            password=args.password,
            ssl=args.ssl,
            tables=args.table,
            plugin=args.plugin,
            status_interval=args.status_interval,
            debounce=args.debounce,
            failsafe=args.failsafe,
        )
    return RawFeed(
        host=args.host,
        port=args.port,
        user=args.user,
        database=args.database,
        password=args.password,
        ssl=args.ssl,
        tables=args.table,
        status_interval=args.status_interval,
        debounce=args.debounce,
        failsafe=args.failsafe,
    )


def _format_batch(batch: Batch) -> str:
    parts = [f"{e.payload} (x{e.count})" if e.count > 1 else e.payload for e in batch.events]
    return "batch: " + ", ".join(parts)


async def _run_doctor(args: argparse.Namespace) -> bool:
    """Print the readiness report; return whether any transport works."""
    diag = await diagnose(
        host=args.host,
        port=args.port,
        user=args.user,
        database=args.database,
        password=args.password,
        ssl=args.ssl,
        plugin=args.plugin,
    )
    for check in diag.checks:
        print(f"[{'ok' if check.ok else 'FAIL'}] {check.name}: {check.detail}", flush=True)
        if not check.ok and check.fix is not None:
            print(f"       fix: {check.fix}", flush=True)
    if diag.recommended is not None:
        print(f"\nrecommended transport: {diag.recommended}", flush=True)
    else:
        print("\nno transport available; fix the FAILed checks above", flush=True)
    return diag.recommended is not None


async def _watch(feed: BaseFeed) -> None:
    async with feed:
        async for item in feed:
            match item:
                case Resync(reason=reason):
                    print(f"resync: {reason}", flush=True)
                case Batch():
                    print(_format_batch(item), flush=True)


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.error("a command is required (e.g. watch, doctor)")
    if args.command == "watch" and args.transport_kind is None:
        parser.error("a transport is required: logical or physical")
    if not args.user:
        parser.error("--user is required (or set PGUSER)")
    if not args.database:
        parser.error("--database is required (or set PGDATABASE)")

    # Install a handler at root but raise only the pgnudge logger; other
    # libraries (asyncio, etc.) stay at WARNING so -vvv isn't drowned out.
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("pgnudge").setLevel(_verbosity_level(args.verbose))

    if args.command == "doctor":
        raise SystemExit(0 if asyncio.run(_run_doctor(args)) else 1)
    try:
        asyncio.run(_watch(_build_feed(args)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
