from __future__ import annotations

import asyncio
from asyncio import get_running_loop
import asyncio.events
import functools
from functools import partial, wraps
import threading
from typing import TYPE_CHECKING, Any, Concatenate, Protocol, cast, overload

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


class _HasLoop(Protocol):
    """An object owning the event loop its coroutines run on (e.g. an async filesystem)."""

    @property
    def loop(self) -> asyncio.AbstractEventLoop: ...


class SyncMethod[S, **P, T](Protocol):
    """Type of a method created by `sync_wrapper`: binds `self` like a regular method."""

    @overload
    def __get__(
        self, instance: None, owner: type | None = None, /
    ) -> Callable[Concatenate[S, P], T]: ...
    @overload
    def __get__(self, instance: S, owner: type | None = None, /) -> Callable[P, T]: ...
    def __call__(self, instance: S, /, *args: P.args, **kwargs: P.kwargs) -> T: ...


def sync_wrapper[S: _HasLoop, **P, T](
    func: Callable[Concatenate[S, P], Coroutine[Any, Any, T]],
) -> SyncMethod[S, P, T]:
    """Make an async method callable in blocking contexts.

    Use in a class body (`ls = sync_wrapper(_ls)`); the coroutine runs on the
    instance's `loop`.
    """

    @functools.wraps(func)
    def wrapper(self: S, /, *args: P.args, **kwargs: P.kwargs) -> T:
        # partial: keeps a wrapped method's own `timeout` kwarg away from `sync()`.
        return sync(self.loop, partial(func, self, *args, **kwargs))

    # A plain function is a descriptor, so it binds exactly as `SyncMethod` says.
    return cast("SyncMethod[S, P, T]", wrapper)


def wrap[**P, R](func: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def run(*args: P.args, **kwargs: P.kwargs) -> R:
        loop = get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, pfunc)

    return run


class AsyncMethod[S, **P, R](Protocol):
    """Type of a method created by `wrap_method`: binds `self` like a regular method."""

    @overload
    def __get__(
        self, instance: None, owner: type | None = None, /
    ) -> Callable[Concatenate[S, P], Awaitable[R]]: ...
    @overload
    def __get__(self, instance: S, owner: type | None = None, /) -> Callable[P, Awaitable[R]]: ...
    def __call__(self, instance: S, /, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]: ...


def wrap_method[S, **P, R](func: Callable[Concatenate[S, P], R]) -> AsyncMethod[S, P, R]:
    """Wrap a sync method for use as an async method in a class body.

    Example: `_ls = wrap_method(Base.ls)`.
    """
    # `wrap` returns a plain (async) function, which binds as `AsyncMethod` says.
    return cast("AsyncMethod[S, P, R]", wrap(func))
