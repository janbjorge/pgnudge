"""Exception hierarchy: every error pgnudge raises inherits ``PgnudgeError``.

A consumer can ``except PgnudgeError`` to catch anything the library
raises. ``ConfigError`` also inherits ``ValueError`` so the existing
``except ValueError`` contract on bad construction arguments still holds.
``PgServerError`` (proto) and ``WalSyncError`` (xlog) inherit the root too;
they stay in their own modules.
"""

__all__ = ["PgnudgeError", "ConfigError"]


class PgnudgeError(Exception):
    """Base for every exception pgnudge raises."""


class ConfigError(PgnudgeError, ValueError):
    """Invalid construction argument or feed configuration.

    Inherits ``ValueError`` so ``except ValueError`` keeps catching it.
    """
