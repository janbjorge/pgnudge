#!/usr/bin/env python
"""Light, continuous DB writes to exercise `pgnudge watch`.

Reads the same libpq PG* env vars as the CLI (PGHOST, PGPORT, PGUSER,
PGDATABASE, PGPASSWORD). Creates two tables and loops doing one small
insert/update/delete per tick so the watcher emits `batch:` lines naming
the tables that moved.

    docker compose up -d --build
    PGHOST=127.0.0.1 PGUSER=pgnudge PGDATABASE=pgnudge PGPASSWORD=pgnudge \
        uv run python scripts/dbio.py

Ctrl-C to stop. --interval sets seconds between ticks (default 1.0).
"""

from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between writes")
    args = ap.parse_args()

    conn = await asyncpg.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER"),
        database=os.environ.get("PGDATABASE"),
        password=os.environ.get("PGPASSWORD"),
    )
    try:
        await conn.execute(
            """
            create table if not exists widgets (id serial primary key, n int not null);
            create table if not exists gadgets (id serial primary key, label text not null);
            """
        )
        print("dbio: connected, writing every", args.interval, "s (Ctrl-C to stop)")

        tick = 0
        while True:
            tick += 1
            # insert into one table, update/delete the other on alternating ticks
            await conn.execute("insert into widgets (n) values ($1)", tick)
            if tick % 2:
                await conn.execute("insert into gadgets (label) values ($1)", f"g{tick}")
            else:
                await conn.execute("delete from gadgets where id = (select min(id) from gadgets)")
                await conn.execute("update widgets set n = n + 1 where id = (select min(id) from widgets)")
            print(f"dbio: tick {tick}")
            await asyncio.sleep(args.interval)
    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
