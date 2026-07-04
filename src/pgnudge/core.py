"""Feed contract: the two item types every feed emits.

``Resync`` (reload everything) and ``Batch`` (coalesced wakeups) —
at-least-once, every gap bracketed by a Resync. See README.
"""

from dataclasses import dataclass
from typing import TypeAlias

__all__ = ["Event", "Batch", "Resync", "FeedItem"]


@dataclass(frozen=True, slots=True)
class Event:
    """One coalesced wakeup; ``count`` = arrivals of this payload in the window."""

    payload: str
    first_seen: float  # time.time() of first arrival in this batch
    count: int = 1


@dataclass(frozen=True, slots=True)
class Batch:
    """A debounce window's worth of events, deduplicated, in arrival order."""

    events: tuple[Event, ...]

    def payloads(self) -> tuple[str, ...]:
        return tuple(e.payload for e in self.events)


@dataclass(frozen=True, slots=True)
class Resync:
    """Reload-everything signal; reason is "connected" | "reconnected" | "overflow" | "failsafe"."""

    reason: str


FeedItem: TypeAlias = Resync | Batch
