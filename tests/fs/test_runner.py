"""Unit tests for :class:`upathtools.fs.LoopRunner` and :func:`get_default_runner`."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import gc
import os
import signal
import sys
import threading
import time
from typing import Any
import warnings

import pytest

from upathtools.fs import (
    FSSPEC_SYNC_VERSION,
    FSTimeoutError,
    LoopRunner,
    get_default_runner,
    runner as runner_mod,
)


def test_runner_result_and_exception_identity() -> None:
    class _CustomError(Exception):
        pass

    sentinel = _CustomError("exact object")
    coro_timeout = TimeoutError("raised inside coroutine")
    coro_fs_timeout = FSTimeoutError("already fs timeout")

    async def compute(x: int) -> int:
        await asyncio.sleep(0)
        return x * 2

    async def fail(exc: BaseException) -> None:
        await asyncio.sleep(0)
        raise exc

    expected = 42
    with LoopRunner() as runner:
        assert not runner.is_running
        assert runner.run(compute(21)) == expected
        assert runner.is_running

        with pytest.raises(_CustomError) as exc_info:
            runner.run(fail(sentinel))
        assert exc_info.value is sentinel

        # A TimeoutError / FSTimeoutError raised by the coroutine itself must propagate
        # unchanged (same object), not be wrapped as a runner timeout.
        with pytest.raises(TimeoutError) as timeout_info:
            runner.run(fail(coro_timeout), timeout=5.0)
        assert timeout_info.value is coro_timeout

        with pytest.raises(FSTimeoutError) as fs_timeout_info:
            runner.run(fail(coro_fs_timeout), timeout=5.0)
        assert fs_timeout_info.value is coro_fs_timeout

    assert not runner.is_running


def test_runner_timeout_raises_fs_timeout_and_cancels_coro() -> None:
    cancelled = threading.Event()
    started = threading.Event()

    async def slow() -> str:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return "done"  # pragma: no cover

    with LoopRunner() as runner:
        with pytest.raises(FSTimeoutError):
            runner.run(slow(), timeout=0.05)

        assert started.is_set()
        assert cancelled.is_set() or cancelled.wait(timeout=1.0)


def test_runner_keyboard_interrupt_cancels_future_and_reraises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    cancelled = threading.Event()

    async def slow() -> None:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    real_submit = asyncio.run_coroutine_threadsafe

    def interrupting_submit(coro: Any, loop: asyncio.AbstractEventLoop) -> Any:
        fut = real_submit(coro, loop)
        orig_result = fut.result

        def _result(timeout: float | None = None) -> Any:
            started.wait(timeout=2.0)
            raise KeyboardInterrupt

        fut.result = _result  # type: ignore[method-assign]
        fut._orig_result = orig_result
        return fut

    with LoopRunner() as runner:
        # Start loop before patching so patching only affects the submitted call.
        _ = runner.loop
        monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", interrupting_submit)
        with pytest.raises(KeyboardInterrupt):
            runner.run(slow())

        assert cancelled.is_set() or cancelled.wait(timeout=1.0)


def test_runner_reentry_from_own_loop_raises_and_no_warning() -> None:
    async def inner() -> int:
        return 1  # pragma: no cover

    with LoopRunner() as runner:

        async def outer() -> None:
            runner.run(inner())

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(
                RuntimeError,
                match="Cannot call the sync facade from the runner's own event loop",
            ):
                runner.run(outer())
            gc.collect()

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert runtime_warnings == []


async def test_runner_works_from_different_running_event_loop() -> None:
    caller_loop = asyncio.get_running_loop()

    async def get_loop_id() -> int:
        return id(asyncio.get_running_loop())

    with LoopRunner() as runner:
        runner_loop_id = runner.run(get_loop_id())
        assert runner_loop_id != id(caller_loop)
        assert runner_loop_id == id(runner.loop)


def test_runner_concurrent_8_threads_400_calls() -> None:
    async def work(tid: int, idx: int) -> tuple[int, int]:
        if idx % 10 == 0:
            await asyncio.sleep(0)
        return (tid, idx)

    num_threads = 8
    calls_per_thread = 50

    def worker(runner: LoopRunner, tid: int) -> list[tuple[int, int]]:
        return [runner.run(work(tid, i)) for i in range(calls_per_thread)]

    with LoopRunner() as runner, ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(worker, runner, tid) for tid in range(num_threads)]
        results = [f.result(timeout=10.0) for f in futures]

    assert len(results) == num_threads
    for tid, items in enumerate(results):
        assert items == [(tid, i) for i in range(calls_per_thread)]


def test_runner_close_idempotent_joins_thread_and_rejects_further_calls() -> None:
    runner = LoopRunner(name="test-close-runner")
    loop = runner.loop
    thread = runner._thread
    assert thread is not None
    assert thread.is_alive()
    assert runner.is_running

    # Schedule a long-running background task directly on the loop to verify close()
    # cancels pending tasks cleanly without hanging.
    task_cancelled = threading.Event()

    async def background() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    asyncio.run_coroutine_threadsafe(background(), loop)
    # Yield briefly so the background task enters sleep().
    runner.run(asyncio.sleep(0.01))

    runner.close()
    assert not thread.is_alive()
    assert not runner.is_running
    assert loop.is_closed()
    assert task_cancelled.is_set()

    # Idempotent second close.
    runner.close()

    # run() and .loop after close raise RuntimeError and do not leak unawaited coro warnings.
    async def noop() -> int:
        return 0  # pragma: no cover

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError, match="closed"):
            runner.run(noop())
        with pytest.raises(RuntimeError, match="closed"):
            _ = runner.loop
        with pytest.raises(RuntimeError, match="closed"), runner:
            pass  # pragma: no cover
        gc.collect()

    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []


def test_runner_close_before_start_spawns_no_thread() -> None:
    runner = LoopRunner(name="never-started")
    assert not runner.is_running
    assert runner._thread is None
    runner.close()
    assert runner._thread is None
    assert not runner.is_running
    runner.close()


def test_runner_reused_across_two_sequential_asyncio_run_calls() -> None:
    with LoopRunner() as runner:

        async def call_runner(val: str) -> str:
            async def echo() -> str:
                await asyncio.sleep(0)
                return f"ok:{val}"

            return runner.run(echo())

        first = asyncio.run(call_runner("one"))
        second = asyncio.run(call_runner("two"))

    assert first == "ok:one"
    assert second == "ok:two"


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
@pytest.mark.skipif(
    sys.platform == "win32" or not hasattr(os, "fork"),
    reason="POSIX os.fork() required",
)
def test_runner_fork_safety() -> None:
    async def ping(tag: str) -> str:
        await asyncio.sleep(0)
        return f"{tag}:{os.getpid()}"

    parent_pid = os.getpid()
    default_runner = get_default_runner()
    explicit_runner = LoopRunner(name="fork-explicit")
    try:
        assert default_runner.run(ping("parent-default")) == f"parent-default:{parent_pid}"
        assert explicit_runner.run(ping("parent-explicit")) == f"parent-explicit:{parent_pid}"

        pid = os.fork()
        if pid == 0:  # pragma: no cover - child process exits via os._exit
            ok = False
            try:
                child_pid = os.getpid()
                r_default = get_default_runner()
                res_default = r_default.run(ping("child-default"), timeout=5.0)
                res_explicit = explicit_runner.run(ping("child-explicit"), timeout=5.0)
                ok = (
                    res_default == f"child-default:{child_pid}"
                    and res_explicit == f"child-explicit:{child_pid}"
                )
                r_default.close()
                explicit_runner.close()
            except BaseException:  # noqa: BLE001 - child must exit via os._exit, never unwind into pytest
                ok = False
            os._exit(0 if ok else 1)

        deadline = time.monotonic() + 10.0
        status: int | None = None
        while time.monotonic() < deadline:
            waited_pid, wait_status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                status = wait_status
                break
            time.sleep(0.02)

        if status is None:  # pragma: no cover
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
            pytest.fail("Forked child process timed out after 10s (likely fork deadlock)")

        assert os.WIFEXITED(status), f"Child did not exit normally: status={status}"
        assert os.WEXITSTATUS(status) == 0

        # Parent's runners must still work after fork.
        assert default_runner.run(ping("after-fork-default")) == f"after-fork-default:{parent_pid}"
        assert (
            explicit_runner.run(ping("after-fork-explicit")) == f"after-fork-explicit:{parent_pid}"
        )
    finally:
        explicit_runner.close()
        default_runner.close()


def test_runner_sync_note() -> None:
    assert runner_mod.__fsspec_sync__.version == FSSPEC_SYNC_VERSION
