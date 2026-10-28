class TransientError(Exception):
    """Worth retrying: network blips, 5xx, rate limits."""


class PermanentError(Exception):
    """Retrying cannot help: bad credentials, broken contract, failed data tests."""


class DriftError(PermanentError):
    """The source no longer matches the declared contract (a key column vanished)."""
