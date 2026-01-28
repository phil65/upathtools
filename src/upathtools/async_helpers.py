from __future__ import annotations

import asyncio
import asyncio.events
import functools
import threading
from typing import TYPE_CHECKING, Any

from fsspec.exceptions import FSTimeoutError


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine


async def _runner(
    event: threading.Event,
    coro: Coroutine[Any, Any, Any],
    result: list[Any],
    timeout: float | None = None,
) -> None:
    timeout = timeout if timeout else None  # convert 0 or 0.0 to None
    awaitable: Awaitable[Any] = coro
    if timeout is not None:
        awaitable = asyncio.wait_for(coro, timeout=timeout)
    try:
        result[0] = await awaitable
    except Exception as ex:  # noqa: BLE001
        result[0] = ex
    finally:
        event.set()


def sync[T](
    loop: asyncio.AbstractEventLoop,
    func: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    timeout: float | None = None,
    **kwargs: Any,
) -> T:
    """Make loop run coroutine until it returns. Runs in other thread.

    Examples
    --------
    >>> fsspec.asyn.sync(fsspec.asyn.get_loop(), func, *args,
                         timeout=timeout, **kwargs)
    """
    timeout = timeout if timeout else None  # convert 0 or 0.0 to None
    # NB: if the loop is not running *yet*, it is OK to submit work
    # and we will wait for it
    if loop.is_closed():
        raise RuntimeError("Loop is not running")
    try:
        loop0 = asyncio.events.get_running_loop()
        if loop0 is loop:
            raise NotImplementedError("Calling sync() from within a running loop")  # noqa: TRY301
    except NotImplementedError:
        raise
    except RuntimeError:
        pass
    coro = func(*args, **kwargs)
    result: list[Any] = [None]
    event = threading.Event()
    asyncio.run_coroutine_threadsafe(_runner(event, coro, result, timeout), loop)
    # this timeout tracks wall-clock time for the threading.Event wait
    remaining_timeout = timeout
    while True:
        # this loop allows thread to get interrupted
        if event.wait(1):
            break
        if remaining_timeout is not None:
            remaining_timeout -= 1
            if remaining_timeout < 0:
                raise FSTimeoutError

    return_result = result[0]
    if isinstance(return_result, asyncio.TimeoutError):
        # suppress asyncio.TimeoutError, raise FSTimeoutError
        raise FSTimeoutError from return_result
    if isinstance(return_result, BaseException):
        raise return_result
    return return_result  # type: ignore[return-value]


def sync_wrapper[**P, T](func: Callable[P, Awaitable[T]], obj: Any = None) -> Callable[P, T]:
    """Given a function, make so can be called in blocking contexts.

    Leave obj=None if defining within a class. Pass the instance if attaching
    as an attribute of the instance.
    """

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = obj or args[0]
        return sync(self.loop, func, *args, **kwargs)  # type: ignore[arg-type, union-attr]

    return wrapper  # type: ignore[return-value]
