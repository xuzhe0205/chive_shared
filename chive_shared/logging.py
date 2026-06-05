"""chive_shared minimal logging — stdlib wrapper with structlog-compatible API.

Both chive and chive-backend configure their own root loggers (structlog or
plain). chive_shared uses stdlib logging internally so it works standalone,
but the wrapper accepts structlog-style keyword arguments so call sites don't
need to change.
"""
import logging
from typing import Any


class _KwargsLogger:
    """Thin wrapper around a stdlib logger that silently accepts extra kwargs.

    structlog call style:  logger.info("event_name", key=value, ...)
    This wrapper formats that as:  "event_name key=value ..."
    so keyword context is still visible in log output.
    """

    def __init__(self, name: str) -> None:
        self._log = logging.getLogger(name)

    def _format(self, event: str, kwargs: dict[str, Any]) -> str:
        if kwargs:
            pairs = " ".join(f"{k}={v}" for k, v in kwargs.items())
            return f"{event} {pairs}"
        return event

    def debug(self, event: str, **kwargs: Any) -> None:
        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(self._format(event, kwargs))

    def info(self, event: str, **kwargs: Any) -> None:
        if self._log.isEnabledFor(logging.INFO):
            self._log.info(self._format(event, kwargs))

    def warning(self, event: str, **kwargs: Any) -> None:
        if self._log.isEnabledFor(logging.WARNING):
            self._log.warning(self._format(event, kwargs))

    def error(self, event: str, **kwargs: Any) -> None:
        if self._log.isEnabledFor(logging.ERROR):
            self._log.error(self._format(event, kwargs))

    def exception(self, event: str, **kwargs: Any) -> None:
        self._log.exception(self._format(event, kwargs))

    def critical(self, event: str, **kwargs: Any) -> None:
        if self._log.isEnabledFor(logging.CRITICAL):
            self._log.critical(self._format(event, kwargs))


def get_logger(name: str) -> _KwargsLogger:
    """Return a stdlib-backed logger with structlog-compatible keyword API."""
    return _KwargsLogger(name)
