"""Private synchronous heartbeat cleanup that preserves chunk failure context."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager


@contextmanager
def _heartbeat_cleanup(stop: Callable[[], None], close_admission: Callable[[], None]) -> Iterator[None]:
    """Close admission before cleanup and retain primary and secondary failures.

    Callers start the heartbeat before entering this scope. The runner never
    holds an admission lock. An ordinary cleanup error cannot replace a fatal
    runner exception; a fatal cleanup error retains its normal propagation.
    This handles synchronous failures only, not background thread propagation.
    """
    primary: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        primary = exc
        close_admission()
        raise
    finally:
        try:
            stop()
        except BaseException as cleanup_error:
            close_admission()
            if primary is None or not isinstance(cleanup_error, Exception):
                raise
            if isinstance(primary, Exception):
                raise RuntimeError(f"{primary}; heartbeat cleanup failed: {cleanup_error}") from primary
            primary.add_note(f"Heartbeat cleanup also failed: {cleanup_error}")
            raise primary
