"""Dedicated, fork-safe background asyncio event-loop runner for ``upathtools.fs``."""

import asyncio
import contextlib
import inspect
import os
import sys
import threading
from typing import TYPE_CHECKING, Any, Self
import weakref

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote
from upathtools.fs.errors import FSTimeoutError


if TYPE_CHECKING:
    from collections.abc import Coroutine


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=(
        "fsspec/asyn.py:get_loop",
        "fsspec/asyn.py:sync",
        "fsspec/asyn.py:reset_after_fork",
    ),
    notes=(
        "Explicit LoopRunner instances with per-instance locks created in __init__ "
        "(never lazily), direct Future.result(timeout) with task cancellation on "
        "timeout/KeyboardInterrupt, same-loop re-entry guard, and fork safety that "
        "resets every registered runner plus the actual module-level locks (fixing "
        "fsspec's `global lock` typo)."
    ),
)

_REENTRY_ERROR = (
    "Cannot call the sync facade from the runner's own event loop; await the AsyncFS method instead"
)

# Module-level locks created eagerly at import time (never lazily).
_default_runner_lock = threading.Lock()
_runners_lock = threading.Lock()
_runners: weakref.WeakSet[LoopRunner] = weakref.WeakSet()
_default_runner: LoopRunner | None = None


def _close_coro(coro: object) -> None:
    """Close an unscheduled coroutine to avoid unawaited-coroutine RuntimeWarnings."""
    if inspect.iscoroutine(coro):
        coro.close()


def _cancel_all_tasks(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel and drain all pending tasks on ``loop`` before closing it."""
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return
    for task in to_cancel:
        task.cancel()
    loop.run_until_complete(asyncio.gather(*to_cancel, return_exceptions=True))
    for task in to_cancel:
        if task.cancelled():
            continue
        exc = task.exception()
        if exc is not None:
            loop.call_exception_handler({
                "message": "Unhandled exception during LoopRunner shutdown",
                "exception": exc,
                "task": task,
            })


class LoopRunner:
    """Owns a single background daemon thread running a private asyncio event loop.

    The thread and event loop are started lazily on first use (:attr:`loop` or
    :meth:`run`) under a per-instance :class:`threading.Lock` created in
    ``__init__``. Instances are tracked in a module-level weak set and reset
    automatically in forked POSIX child processes.

    Example::

        with LoopRunner() as runner:
            result = runner.run(my_coro(), timeout=5.0)
    """

    __slots__ = (
        "__weakref__",
        "_closed",
        "_lock",
        "_loop",
        "_name",
        "_thread",
    )

    def __init__(self, *, name: str = "upathtools-io") -> None:
        """Initialize a lazy :class:`LoopRunner`.

        Args:
            name: Name assigned to the background daemon thread once started.
        """
        self._name = name
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._closed = False
        with _runners_lock:
            _runners.add(self)

    @property
    def is_running(self) -> bool:
        """Return True when the background loop thread is started and running."""
        with self._lock:
            return (
                not self._closed
                and self._loop is not None
                and self._thread is not None
                and self._thread.is_alive()
                and self._loop.is_running()
            )

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Return the runner's event loop, starting the background thread if needed.

        Raises:
            RuntimeError: If this runner has been closed.
        """
        return self._ensure_started()

    def _ensure_started(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._closed:
                raise RuntimeError("LoopRunner is closed")
            if self._loop is not None and self._thread is not None and self._thread.is_alive():
                return self._loop

            ready = threading.Event()
            loop_holder: list[asyncio.AbstractEventLoop] = []
            error_holder: list[BaseException] = []

            def _thread_main() -> None:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop_holder.append(loop)
                    loop.call_soon(ready.set)
                except BaseException as exc:  # noqa: BLE001  # pragma: no cover
                    error_holder.append(exc)
                    ready.set()
                    return
                try:
                    loop.run_forever()
                finally:
                    try:
                        _cancel_all_tasks(loop)
                        loop.run_until_complete(loop.shutdown_asyncgens())
                        loop.run_until_complete(loop.shutdown_default_executor())
                    finally:
                        asyncio.set_event_loop(None)
                        loop.close()

            thread = threading.Thread(target=_thread_main, name=self._name, daemon=True)
            thread.start()
            ready.wait()
            if error_holder:  # pragma: no cover
                raise error_holder[0]
            loop = loop_holder[0]
            self._loop = loop
            self._thread = thread
            return loop

    @staticmethod
    def _flush_cancellation(loop: asyncio.AbstractEventLoop) -> None:
        """Wait for two loop turns so a queued ``task.cancel()`` delivers ``CancelledError``."""
        settled = threading.Event()

        def _after_cancel() -> None:
            loop.call_soon(settled.set)

        try:
            loop.call_soon_threadsafe(_after_cancel)
        except RuntimeError:
            return
        settled.wait(timeout=0.5)

    def _resolve_loop_for_run(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._closed:
                raise RuntimeError("LoopRunner is closed")
            if self._thread is not None and threading.current_thread() is self._thread:
                raise RuntimeError(_REENTRY_ERROR)
            loop = self._loop if (self._thread is not None and self._thread.is_alive()) else None
        return self._ensure_started() if loop is None else loop

    def run[T](
        self,
        coro: Coroutine[Any, Any, T],
        *,
        timeout: float | None = None,
    ) -> T:
        """Submit ``coro`` to the runner's event loop and block until it completes.

        Can be called safely from any thread other than the runner's own loop thread,
        including threads running a different asyncio event loop.

        Args:
            coro: Coroutine object to execute on the runner's event loop.
            timeout: Optional timeout in seconds. When exceeded, the task is
                cancelled on the loop and :class:`FSTimeoutError` is raised.

        Returns:
            The value returned by ``coro``.

        Raises:
            RuntimeError: If the runner is closed or if called from the runner's
                own event loop thread.
            FSTimeoutError: If ``timeout`` expires before ``coro`` completes.
        """
        try:
            loop = self._resolve_loop_for_run()
            future = asyncio.run_coroutine_threadsafe(coro, loop)
        except BaseException:
            _close_coro(coro)
            raise

        try:
            return future.result(timeout=timeout)
        except TimeoutError as exc:
            # Distinguish a TimeoutError raised by the coroutine itself from a wait timeout.
            if future.done() and not future.cancelled() and future.exception() is exc:
                raise
            future.cancel()
            self._flush_cancellation(loop)
            raise FSTimeoutError(f"Operation timed out after {timeout}s") from exc
        except KeyboardInterrupt:
            future.cancel()
            self._flush_cancellation(loop)
            raise

    def close(self) -> None:
        """Cancel pending tasks, stop and close the event loop, and join its thread.

        Idempotent: subsequent calls after the runner is closed are no-ops.
        """
        with self._lock:
            self._closed = True
            loop = self._loop
            thread = self._thread
            self._loop = None

        if loop is not None:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)

        if thread is not None and thread is not threading.current_thread():
            thread.join()

        with self._lock:
            if self._thread is thread:
                self._thread = None

    def _reset_after_fork(self) -> None:
        """Reset inherited parent loop/thread references in a forked child process."""
        # Do not touch or close self._loop / self._thread: they belonged to the
        # parent process and their background thread does not exist in the child.
        self._lock = threading.Lock()
        self._loop = None
        self._thread = None

    def __enter__(self) -> Self:
        with self._lock:
            if self._closed:
                raise RuntimeError("LoopRunner is closed")
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def get_default_runner() -> LoopRunner:
    """Return the process-wide default :class:`LoopRunner` for scripts and REPL use.

    Created lazily on first call and reset automatically in forked POSIX child
    processes.

    Note:
        Library code and async applications should not rely on the process-wide
        default runner: prefer awaiting :class:`~upathtools.fs.AsyncFS` directly
        when inside async code, or construct and manage a dedicated
        :class:`LoopRunner` when bridging to synchronous code.
    """
    global _default_runner
    with _default_runner_lock:
        if _default_runner is None or _default_runner._closed:
            _default_runner = LoopRunner(name="upathtools-default-io")
        return _default_runner


def _before_fork() -> None:
    _default_runner_lock.acquire()
    _runners_lock.acquire()


def _after_fork_in_parent() -> None:
    _runners_lock.release()
    _default_runner_lock.release()


def _reset_after_fork() -> None:
    """Reset module locks, default runner, and all live LoopRunner instances in child."""
    global _default_runner, _default_runner_lock, _runners_lock
    _default_runner_lock = threading.Lock()
    _runners_lock = threading.Lock()
    _default_runner = None
    for runner in list(_runners):
        runner._reset_after_fork()


if sys.platform != "win32" and hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_before_fork,
        after_in_parent=_after_fork_in_parent,
        after_in_child=_reset_after_fork,
    )
