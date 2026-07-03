"""Minimal end-to-end pgwake example (see README for server config)."""
import asyncio
from pgwake import Batch, Resync, WalFeed

async def main() -> None:
    async with WalFeed(
        host="localhost", user="wal_user", password="sekret", database="mydb",
        tables=["public.orders", "public.stations"],
    ) as feed:
        async for item in feed:
            match item:
                case Resync(reason=r):
                    print(f"[resync:{r}] reload everything")
                case Batch(events=evs):
                    print(f"[batch] changed: {[e.payload for e in evs]}")

asyncio.run(main())
