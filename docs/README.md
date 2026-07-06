# pgnudge docs

Three pages, two audiences.

For users deciding whether and how to run pgnudge:

- [temporary-slots.md](temporary-slots.md): the logical transport
  (`WalFeed`). Why temporary slots, the gap-free handshake, coalescing,
  polling comparison, and when you should not use pgnudge.
- [physical-wal.md](physical-wal.md): the physical transport (`RawFeed`).
  Why it exists, commit gating, name resolution, and the gaps stated
  plainly.

For contributors working on the parsers:

- [parsing.md](parsing.md): wire frames, WAL record and page layouts,
  and the structures each layer produces, byte by byte.
