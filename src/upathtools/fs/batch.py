"""Bounded async concurrency primitive for ``upathtools.fs``."""

import asyncio
from collections import deque
import inspect
from typing import TYPE_CHECKING, Any, Literal, overload

from upathtools.fs._sync import FSSPEC_SYNC_VERSION, FsspecSyncNote


if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterable


__fsspec_sync__ = FsspecSyncNote(
    version=FSSPEC_SYNC_VERSION,
    upstream=("fsspec/asyn.py:_run_coros_in_chunks",),
    notes=(
        "Structured concurrency rewrite using an asyncio.TaskGroup worker pool. "
        "Supports explicit concurrency limit (None = unbounded), unwraps single "
        "failure instead of raising ExceptionGroup, closes any unstarted coroutines "
        "on error or cancellation, and never swallows caller CancelledError."
    ),
)


def _close_coroutines(awaitables: Iterable[Awaitable[Any]]) -> None:
    """Close any unstarted coroutine objects to prevent unawaited-coroutine warnings."""
    for item in awaitables:
        if inspect.iscoroutine(item):
            item.close()


@overload
async def run_bounded[T](
    coros: Iterable[Awaitable[T]],
    *,
    limit: int | None = None,
    return_exceptions: Literal[False] = False,
) -> list[T]: ...


@overload
async def run_bounded[T](
    coros: Iterable[Awaitable[T]],
    *,
    limit: int | None = None,
    return_exceptions: Literal[True],
) -> list[T | BaseException]: ...


@overload
async def run_bounded[T](
    coros: Iterable[Awaitable[T]],
    *,
    limit: int | None = None,
    return_exceptions: bool,
) -> list[T | BaseException]: ...


async def run_bounded[T](
    coros: Iterable[Awaitable[T]],
    *,
    limit: int | None = None,
    return_exceptions: bool = False,
) -> list[T] | list[T | BaseException]:
    """Run awaitables concurrently with an optional upper bound on active tasks.

    Results are always returned in input order. When ``return_exceptions`` is
    ``False``, the first failure cancels all remaining running tasks, closes any
    unstarted coroutines, and raises that exception directly (unwrapped from
    ``ExceptionGroup``). When ``return_exceptions`` is ``True``, exceptions raised
    by individual awaitables are placed at their corresponding index in the result
    list while cancellation of the caller still cancels all child tasks and
    propagates ``asyncio.CancelledError``.

    Args:
        coros: Iterable of awaitables to execute.
        limit: Maximum number of awaitables running concurrently, or ``None`` for
            no limit. Must be ``>= 1`` if provided.
        return_exceptions: If ``True``, capture exceptions in the returned list
            instead of aborting on the first failure.

    Returns:
        List of results (or results and exceptions) in the same order as ``coros``.

    Raises:
        ValueError: If ``limit`` is not ``None`` and ``limit <= 0``.
    """
    items: list[Awaitable[T]] = []
    if isinstance(coros, (list, tuple)):
        items = list(coros)
    else:
        try:
            for coro in coros:
                items.append(coro)
        except BaseException:
            _close_coroutines(items)
            raise

    if limit is not None and limit <= 0:
        _close_coroutines(items)
        raise ValueError(f"limit must be a positive integer or None, got {limit}")

    count = len(items)
    if count == 0:
        return []

    results: list[Any] = [None] * count
    pending: deque[tuple[int, Awaitable[T]]] = deque(enumerate(items))
    first_exc: BaseException | None = None

    async def _worker() -> None:
        nonlocal first_exc
        while pending:
            idx, awaitable = pending.popleft()
            if return_exceptions:
                try:
                    results[idx] = await awaitable
                except asyncio.CancelledError as exc:
                    current = asyncio.current_task()
                    if current is not None and current.cancelling() > 0:
                        raise
                    results[idx] = exc
                except Exception as exc:  # noqa: BLE001
                    # Only errors of the awaitable: KeyboardInterrupt / SystemExit propagate.
                    results[idx] = exc
            else:
                try:
                    results[idx] = await awaitable
                except BaseException as exc:
                    if not isinstance(exc, asyncio.CancelledError) and first_exc is None:
                        first_exc = exc
                    raise

    worker_count = count if limit is None else min(limit, count)
    try:
        async with asyncio.TaskGroup() as tg:
            for _ in range(worker_count):
                tg.create_task(_worker())
    except BaseExceptionGroup as eg:
        exc_to_raise = first_exc if first_exc is not None else eg.exceptions[0]
        raise exc_to_raise from exc_to_raise.__cause__
    finally:
        if pending:
            _close_coroutines(awaitable for _, awaitable in pending)
            pending.clear()

    return results
