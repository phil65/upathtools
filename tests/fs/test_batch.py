import asyncio
import gc
import warnings

import pytest

from upathtools.fs import FSSPEC_SYNC_VERSION, batch as batch_mod, run_bounded


async def test_run_bounded_enforces_concurrency_limit_and_preserves_order() -> None:
    active = 0
    peak = 0
    limit = 3

    async def task(idx: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            # Later indices sleep less so completion order differs from input order.
            await asyncio.sleep(0.01 * (5 - (idx % 5)))
            return idx * 10
        finally:
            active -= 1

    results = await run_bounded((task(i) for i in range(15)), limit=limit)
    assert results == [i * 10 for i in range(15)]
    assert peak == limit


async def test_run_bounded_unbounded_and_empty() -> None:
    assert await run_bounded([]) == []
    assert await run_bounded([], limit=4) == []

    total = 6
    active = 0
    peak = 0
    gate = asyncio.Event()

    async def task(idx: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == total:
            gate.set()
        await gate.wait()
        active -= 1
        return idx

    results = await run_bounded([task(i) for i in range(total)], limit=None)
    assert results == list(range(total))
    assert peak == total


async def test_run_bounded_first_error_cancels_others_and_unwraps() -> None:
    started = asyncio.Event()
    saw_cancelled = False
    root_cause = KeyError("missing")

    async def failing() -> int:
        await started.wait()
        raise RuntimeError("boom") from root_cause

    async def sibling() -> int:
        nonlocal saw_cancelled
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            saw_cancelled = True
            raise
        return 1

    async def never_started() -> int:
        return 2

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        coros = [failing(), sibling(), never_started(), never_started()]
        with pytest.raises(RuntimeError, match="boom") as exc_info:
            await run_bounded(coros, limit=2)
        del coros
        gc.collect()

    assert not isinstance(exc_info.value, BaseExceptionGroup)
    assert exc_info.value.__cause__ is root_cause
    assert saw_cancelled is True


async def test_run_bounded_return_exceptions_captures_in_place() -> None:
    async def ok(val: int) -> int:
        await asyncio.sleep(0.005)
        return val

    async def fail_value() -> int:
        await asyncio.sleep(0.002)
        raise ValueError("bad")

    async def fail_child_cancelled() -> int:
        raise asyncio.CancelledError("child-only")

    expected_third = 3
    results = await run_bounded(
        [ok(1), fail_value(), ok(expected_third), fail_child_cancelled()],
        limit=2,
        return_exceptions=True,
    )
    assert results[0] == 1
    assert isinstance(results[1], ValueError)
    assert str(results[1]) == "bad"
    assert results[2] == expected_third
    assert isinstance(results[3], asyncio.CancelledError)


@pytest.mark.parametrize("return_exceptions", [False, True])
async def test_run_bounded_caller_cancellation_cancels_children(
    return_exceptions: bool,
) -> None:
    limit = 2
    all_started = asyncio.Event()
    started_count = 0
    cancelled_count = 0

    async def sleeper() -> int:
        nonlocal started_count, cancelled_count
        started_count += 1
        if started_count == limit:
            all_started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled_count += 1
            raise
        return 0

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        coros = [sleeper() for _ in range(5)]
        runner = asyncio.create_task(
            run_bounded(coros, limit=limit, return_exceptions=return_exceptions)
        )
        await all_started.wait()
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner
        del coros
        gc.collect()

    assert cancelled_count == limit


@pytest.mark.parametrize("bad_limit", [0, -1, -10])
async def test_run_bounded_invalid_limit_raises_and_closes_coros(bad_limit: int) -> None:
    async def noop() -> int:
        return 1

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        coros = [noop(), noop()]
        with pytest.raises(ValueError, match="limit"):
            await run_bounded(coros, limit=bad_limit)
        del coros
        gc.collect()


async def test_run_bounded_closes_coros_if_iterable_raises_midway() -> None:
    async def noop() -> int:
        return 1

    def broken_gen():
        yield noop()
        yield noop()
        raise RuntimeError("iterator failed")

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        with pytest.raises(RuntimeError, match="iterator failed"):
            await run_bounded(broken_gen(), limit=2)
        gc.collect()


async def test_run_bounded_supports_non_coroutine_awaitables() -> None:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[int] = loop.create_future()
    fut.set_result(42)

    async def coro() -> int:
        return 7

    results = await run_bounded([fut, coro()], limit=2)
    assert results == [42, 7]


def test_batch_sync_note() -> None:
    assert batch_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION
    assert "fsspec/asyn.py:_run_coros_in_chunks" in batch_mod.__fsspec_sync__.upstream
