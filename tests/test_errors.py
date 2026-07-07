"""Exception hierarchy: every pgnudge error is a ``PgnudgeError``, and
``ConfigError`` stays ``ValueError``-compatible for the old catch contract.
"""

from pgnudge import ConfigError, PgnudgeError, PgServerError, WalSyncError


def test_config_error_is_pgnudge_and_valueerror() -> None:
    err = ConfigError("bad")
    assert isinstance(err, PgnudgeError)
    assert isinstance(err, ValueError)  # the non-breaking compat contract


def test_pg_server_error_under_root() -> None:
    assert isinstance(PgServerError.from_message("x"), PgnudgeError)


def test_wal_sync_error_under_root() -> None:
    assert isinstance(WalSyncError("desync"), PgnudgeError)


def test_root_is_plain_exception() -> None:
    assert issubclass(PgnudgeError, Exception)
