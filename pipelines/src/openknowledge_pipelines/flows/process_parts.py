"""Bounded spawned execution; cancellation waits for writes before releasing admission."""

import asyncio
import multiprocessing
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import NoReturn

from openknowledge_core.ingestion.errors import IngestionError
from openknowledge_core.search.commands import IndexPapers, IndexPapersResult


class PartWorkerLost(BaseException):
    """External requests may survive a lost child; admission must stay occupied."""


class _PartPool(ProcessPoolExecutor):
    def _spawn_process(self) -> None:
        # CPython 3.12 submit() wakes the manager BEFORE registering a new child.
        # If it consumes that wakeup during spawn, it can wait on the old sentinel
        # list and miss this child's death until a sibling returns. Wake it again
        # after registration. These private hooks are tied to the pinned runtime.
        super()._spawn_process()  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
        self._executor_manager_thread_wakeup.wakeup()


class PartProcesses:
    def __init__(
        self, worker: Callable[[IndexPapers], IndexPapersResult], parallelism: int
    ) -> None:
        self._shutdown: asyncio.Task[None] | None = None
        self._worker = worker
        self._slots = asyncio.Semaphore(parallelism)
        self._pool = _PartPool(
            max_workers=parallelism, mp_context=multiprocessing.get_context("spawn")
        )

    async def index(self, command: IndexPapers) -> IndexPapersResult:
        async with self._slots:
            try:
                future = asyncio.get_running_loop().run_in_executor(
                    self._pool, self._worker, command
                )
            except BrokenProcessPool:
                await self._lost()

            interrupted = False
            while True:
                try:
                    result = await asyncio.shield(future)
                    break
                except asyncio.CancelledError:
                    if future.done() and (
                        future.cancelled() or isinstance(future.exception(), asyncio.CancelledError)
                    ):
                        await self._lost()
                    # Cancelling an asyncio Future cannot stop a running child. Keep its
                    # admission slot until it exits, even under repeated cancellation.
                    interrupted = True
                except BrokenProcessPool:
                    await self._lost()
                except IngestionError as exc:
                    if interrupted:
                        raise asyncio.CancelledError from None
                    raise type(exc)(str(exc)) from None
                except Exception:
                    if interrupted:
                        raise asyncio.CancelledError from None
                    raise RuntimeError(
                        "Index part worker failed; rerun the unfinished part"
                    ) from None
            if interrupted:
                raise asyncio.CancelledError
            return result

    async def _lost(self) -> NoReturn:
        # CPython 3.12 _terminate_broken resolves futures BEFORE terminating and
        # joining siblings. Join the manager thread before unwinding admission.
        # Even after joining, a killed client's server-side request is uncertain.
        await self._join()
        raise PartWorkerLost(
            "Index worker lost; admission retained. Reconcile child processes and "
            "server-side requests before releasing slots and resuming"
        ) from None

    async def _join(self) -> bool:
        if self._shutdown is None:
            self._shutdown = asyncio.create_task(asyncio.to_thread(self._pool.shutdown, wait=True))
        interrupted = False
        while not self._shutdown.done():
            try:
                await asyncio.shield(self._shutdown)
            except asyncio.CancelledError:
                interrupted = True
        self._shutdown.result()
        return interrupted

    async def close(self) -> None:
        if await self._join():
            raise asyncio.CancelledError
