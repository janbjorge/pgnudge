"""Push-only change nudges from PostgreSQL; nothing on the server outlives the connection."""

from importlib.metadata import PackageNotFoundError, version

from pgnudge.core import Batch, Event, FeedItem, Resync
from pgnudge.errors import ConfigError, PgnudgeError
from pgnudge.proto import PgServerError
from pgnudge.raw import RawFeed
from pgnudge.wal import WalFeed
from pgnudge.xlog import WalSyncError

try:
    __version__ = version("pgnudge")
except PackageNotFoundError:  # running from a bare source tree, never installed
    __version__ = "0.0.0"

__all__ = [
    "Batch",
    "ConfigError",
    "Event",
    "FeedItem",
    "PgServerError",
    "PgnudgeError",
    "RawFeed",
    "Resync",
    "WalFeed",
    "WalSyncError",
    "__version__",
]
