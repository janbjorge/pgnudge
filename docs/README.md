# pgnudge docs

Four pages, two audiences.

For users deciding whether and how to run pgnudge:

- [temporary-slots.md](temporary-slots.md): the logical transport
  (`WalFeed`). Why temporary slots, the gap-free handshake, coalescing,
  polling comparison, and when you should not use pgnudge.
- [physical-wal.md](physical-wal.md): the physical transport (`RawFeed`).
  Why it exists, commit gating, name resolution, and the gaps stated
  plainly.

For contributors working on the parsers, read in this order:

1. [parsing-walkthrough.md](parsing-walkthrough.md): the guided tour.
   Follow one row change from the wire to the `schema.table` string a
   consumer sees, one step at a time. Start here to build the model.
2. [parsing-reference.md](parsing-reference.md): the byte-level reference. Wire frames, WAL
   record and page layouts, and the structures each layer produces,
   field by field. The lookup table once the model is in place.
