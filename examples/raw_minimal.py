"""Minimal RawFeed example: physical WAL, works at wal_level=replica."""
import asyncio
from pgnudge import Batch, RawFeed, Resync

async def main() -> None:
    async with RawFeed(
        host="localhost", user="replication_user", password="sekret", database="mydb",
        tables=["public.orders", "public.stations"],
    ) as feed:
        async for item in feed:
            match item:
                case Resync(reason=r):
                    print(f"[resync:{r}] reload everything")
                case Batch(events=evs):
                    print(f"[batch] changed: {[e.payload for e in evs]}")

asyncio.run(main())
