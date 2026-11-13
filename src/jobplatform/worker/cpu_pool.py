"""Process pool for CPU-bound handlers with cooperative cancellation.

Each running CPU job owns a slot in a shared flag array. Setting the flag from the parent
makes the child's next `check()` raise JobCancelledError. Slots are handed out from a queue,
so concurrent CPU jobs never share a flag, and a slot is only recycled after the child
function has actually returned.
"""

from __future__ import annotations

import asyncio
import functools
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from jobplatform.domain.errors import JobCancelledError
from jobplatform.worker.handlers.cpu import CPU_HANDLERS

_FLAGS: Any = None


def _init_child(flags: Any) -> None:
    global _FLAGS
    _FLAGS = flags


class CancelToken:
    def __init__(self, slot: int) -> None:
        self._slot = slot

    def is_set(self) -> bool:
        return _FLAGS is not None and bool(_FLAGS[self._slot])

    def check(self) -> None:
        if self.is_set():
            raise JobCancelledError("cancelled")


def run_cpu_job(name: str, payload: dict[str, Any], attempt: int, slot: int) -> dict[str, Any]:
    """Entry point executed inside the child process."""
    return CPU_HANDLERS[name](payload, attempt, CancelToken(slot))


class CpuPool:
    def __init__(self, processes: int) -> None:
        ctx = mp.get_context("spawn")
        self._flags = ctx.Array("b", processes, lock=False)
        self._executor = ProcessPoolExecutor(
            max_workers=processes,
            mp_context=ctx,
            initializer=_init_child,
            initargs=(self._flags,),
        )
        self._slots: asyncio.Queue[int] = asyncio.Queue()
        self._outstanding: set[int] = set()
        for i in range(processes):
            self._slots.put_nowait(i)
        self.size = processes

    async def run(self, name: str, payload: dict[str, Any], attempt: int) -> dict[str, Any]:
        slot = await self._slots.get()
        self._outstanding.add(slot)
        self._flags[slot] = 0
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self._executor, run_cpu_job, name, payload, attempt, slot)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # Ask the child to stop at its next check, then let it unwind before the slot
            # is reused. The caller is already being cancelled, so do not block on it.
            self._flags[slot] = 1
            future.add_done_callback(functools.partial(self._on_done, slot))
            raise
        finally:
            if future.done():
                self._recycle(slot)

    def _on_done(self, slot: int, _future: asyncio.Future[Any]) -> None:
        self._recycle(slot)

    def _recycle(self, slot: int) -> None:
        if slot in self._outstanding:
            self._outstanding.discard(slot)
            self._flags[slot] = 0
            self._slots.put_nowait(slot)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
