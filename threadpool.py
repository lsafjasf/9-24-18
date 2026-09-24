"""A fixed-size priority thread pool with bounded queue and two-phase shutdown.

Standard library only (threading / heapq / enum / time).

Design notes
------------
* Fixed number of worker threads, created lazily at pool construction.
* Tasks are dequeued by priority (smaller value first), FIFO within the
  same priority, implemented with a heap guarded by one lock + two
  condition variables (not_empty / not_full).
* Bounded queue: when full, submit() either blocks (optionally with a
  timeout) or raises PoolError(QUEUE_FULL) immediately.
* Graceful shutdown: stop accepting submissions, drain the queue, let
  in-flight tasks finish, then join workers.
* Abort shutdown: pending tasks are marked ABORTED (identifiable via
  handle.state and the PoolError(TASK_ABORTED) raised by result());
  already-running tasks are allowed to finish because Python cannot
  safely kill threads.
* Deadlock avoidance:
  - submit() during/after shutdown raises POOL_SHUTDOWN instead of
    blocking; blocked submitters are woken up on shutdown and fail fast.
  - result(timeout=...) raises TASK_TIMEOUT, so tasks that wait on each
    other inside the pool can always unwind; a documented timeout is the
    contract for intra-pool waits (an unbounded cyclic wait on a fixed
    pool is unsolvable in general).
* Exceptions raised inside a task are captured and re-raised to the
  submitter via result()/exception(); the worker thread keeps running.
* Memory: queue entries are popped (not referenced) once executed, and
  the callable/args are released right after the task finishes, so
  neither the pool nor the handle retains user payloads.
"""

from __future__ import annotations

import heapq
import itertools
import threading
import time
from enum import Enum

__all__ = [
    "ErrorCode",
    "PoolError",
    "TaskState",
    "TaskHandle",
    "ThreadPool",
]


class ErrorCode(Enum):
    QUEUE_FULL = "QUEUE_FULL"
    POOL_SHUTDOWN = "POOL_SHUTDOWN"
    TASK_ABORTED = "TASK_ABORTED"
    TASK_CANCELLED = "TASK_CANCELLED"
    TASK_TIMEOUT = "TASK_TIMEOUT"


class PoolError(Exception):
    """All pool-originated errors carry a machine-readable ``code``."""

    def __init__(self, code: ErrorCode, message: str = ""):
        self.code = code
        super().__init__(message or code.value)

    def __repr__(self):  # pragma: no cover - cosmetic
        return f"PoolError({self.code.value}: {self})"


class TaskState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    ABORTED = "ABORTED"


class TaskHandle:
    """Future-like handle returned by submit()."""

    __slots__ = ("_pool", "_cond", "state", "_result", "_exception", "__weakref__")

    def __init__(self, pool: "ThreadPool"):
        self._pool = pool
        self._cond = threading.Condition()
        self.state = TaskState.PENDING
        self._result = None
        self._exception = None

    # -- introspection -------------------------------------------------
    def done(self) -> bool:
        return self.state not in (TaskState.PENDING, TaskState.RUNNING)

    # -- cancellation --------------------------------------------------
    def cancel(self) -> bool:
        """Cancel a still-pending task. Returns True on success.

        Safe to call at any time, including during/after shutdown; a
        running or finished task simply reports False. The queue slot is
        reclaimed lazily when the worker pops the tombstone.
        """
        with self._pool._lock:
            if self.state is not TaskState.PENDING:
                return False
            self._set_state_locked(TaskState.CANCELLED)
            return True

    # -- result retrieval ----------------------------------------------
    def result(self, timeout: float | None = None):
        """Wait for the outcome.

        Raises the task's own exception if it failed, PoolError for
        CANCELLED/ABORTED tasks, and PoolError(TASK_TIMEOUT) if the
        timeout elapses. A timeout only releases *this* waiter; it never
        cancels or interrupts the task itself.
        """
        self._wait(timeout)
        if self.state is TaskState.SUCCEEDED:
            return self._result
        if self.state is TaskState.FAILED:
            raise self._exception
        if self.state is TaskState.CANCELLED:
            raise PoolError(ErrorCode.TASK_CANCELLED, "task was cancelled")
        raise PoolError(ErrorCode.TASK_ABORTED, "task was aborted by shutdown")

    def exception(self, timeout: float | None = None):
        """Return the task's exception, or None if it succeeded."""
        self._wait(timeout)
        if self.state is TaskState.FAILED:
            return self._exception
        if self.state is TaskState.CANCELLED:
            raise PoolError(ErrorCode.TASK_CANCELLED, "task was cancelled")
        if self.state is TaskState.ABORTED:
            raise PoolError(ErrorCode.TASK_ABORTED, "task was aborted by shutdown")
        return None

    def _wait(self, timeout: float | None) -> None:
        with self._cond:
            if self.done():
                return
            deadline = None if timeout is None else time.monotonic() + timeout
            while not self.done():
                if deadline is None:
                    self._cond.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolError(
                        ErrorCode.TASK_TIMEOUT,
                        f"task not finished within {timeout}s",
                    )
                self._cond.wait(remaining)

    # -- internal transitions (called with pool._lock or worker-side) --
    def _set_state_locked(self, state: TaskState, result=None, exception=None):
        self.state = state
        self._result = result
        self._exception = exception
        with self._cond:
            self._cond.notify_all()


class _Task:
    """Queue entry: keeps the payload separate from the public handle so
    the handle stays lightweight and the payload can be freed eagerly."""

    __slots__ = ("handle", "fn", "args", "kwargs")

    def __init__(self, handle, fn, args, kwargs):
        self.handle = handle
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def release_payload(self):
        self.fn = None
        self.args = None
        self.kwargs = None


class ThreadPool:
    def __init__(self, workers: int, max_queue: int, name: str = "pool"):
        if workers < 1:
            raise ValueError("workers must be >= 1")
        if max_queue < 1:
            raise ValueError("max_queue must be >= 1")
        self._max_queue = max_queue
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._heap: list = []  # (priority, seq, _Task)
        self._seq = itertools.count()
        self._shutdown = False
        self._aborted = False
        self._threads = [
            threading.Thread(
                target=self._worker, name=f"{name}-{i}", daemon=True
            )
            for i in range(workers)
        ]
        for t in self._threads:
            t.start()

    # -- submission ----------------------------------------------------
    def submit(
        self,
        fn,
        *args,
        priority: int = 0,
        block: bool = True,
        timeout: float | None = None,
        **kwargs,
    ) -> TaskHandle:
        """Enqueue fn(*args, **kwargs). Smaller priority runs first.

        Queue-full policy: block=True waits (optionally up to ``timeout``
        seconds) for a slot; block=False raises PoolError(QUEUE_FULL)
        immediately. Submitting after shutdown raises
        PoolError(POOL_SHUTDOWN) and never blocks.
        """
        handle = TaskHandle(self)
        task = _Task(handle, fn, args, kwargs)
        with self._not_full:
            if self._shutdown:
                raise PoolError(ErrorCode.POOL_SHUTDOWN, "pool is shut down")
            deadline = None if timeout is None else time.monotonic() + timeout
            while len(self._heap) >= self._max_queue:
                if not block:
                    raise PoolError(
                        ErrorCode.QUEUE_FULL,
                        f"queue full (max_queue={self._max_queue})",
                    )
                if deadline is None:
                    self._not_full.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolError(
                            ErrorCode.QUEUE_FULL,
                            f"queue still full after {timeout}s",
                        )
                    self._not_full.wait(remaining)
                if self._shutdown:
                    # Woken by shutdown(): fail fast instead of blocking
                    # forever on a queue that will never drain for us.
                    raise PoolError(ErrorCode.POOL_SHUTDOWN, "pool is shut down")
            heapq.heappush(self._heap, (priority, next(self._seq), task))
            self._not_empty.notify()
        return handle

    # -- shutdown --------------------------------------------------------
    def shutdown(self, wait: bool = True, abort: bool = False) -> None:
        """Idempotent two-phase shutdown.

        abort=False (graceful): stop accepting tasks, drain the queue,
        finish in-flight work, then exit workers.
        abort=True: additionally mark every pending task ABORTED and
        skip it; running tasks are allowed to finish (threads cannot be
        killed safely in CPython). A later call may escalate a graceful
        shutdown to abort; repeated calls are safe.
        """
        with self._not_empty:
            if not self._shutdown:
                self._shutdown = True
            if abort and not self._aborted:
                self._aborted = True
                while self._heap:
                    _, _, task = heapq.heappop(self._heap)
                    if task.handle.state is TaskState.PENDING:
                        task.handle._set_state_locked(TaskState.ABORTED)
                    task.release_payload()
            self._not_empty.notify_all()
            self._not_full.notify_all()  # wake blocked submitters
        if wait:
            self.join()

    def join(self, timeout: float | None = None) -> bool:
        """Wait for all workers to exit. Returns True if they did."""
        deadline = None if timeout is None else time.monotonic() + timeout
        for t in self._threads:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            t.join(remaining)
        alive = [t for t in self._threads if t.is_alive()]
        if not alive:
            self._threads.clear()  # release Thread objects
        return not alive

    # -- introspection ---------------------------------------------------
    @property
    def queue_size(self) -> int:
        with self._lock:
            return len(self._heap)

    @property
    def is_shutdown(self) -> bool:
        with self._lock:
            return self._shutdown

    def __enter__(self) -> "ThreadPool":
        return self

    def __exit__(self, *exc_info) -> None:
        self.shutdown(wait=True)

    # -- worker ----------------------------------------------------------
    def _worker(self) -> None:
        while True:
            with self._not_empty:
                while not self._heap and not self._shutdown:
                    self._not_empty.wait()
                if not self._heap:
                    # shutdown set and queue drained -> exit
                    return
                _, _, task = heapq.heappop(self._heap)
                self._not_full.notify()
                handle = task.handle
                if handle.state is not TaskState.PENDING:
                    # cancelled (or aborted) tombstone: reclaim slot, skip
                    task.release_payload()
                    continue
                handle.state = TaskState.RUNNING
            try:
                result = task.fn(*task.args, **task.kwargs)
            except BaseException as exc:  # never let a task kill the worker
                with self._lock:
                    handle._set_state_locked(TaskState.FAILED, exception=exc)
            else:
                with self._lock:
                    handle._set_state_locked(TaskState.SUCCEEDED, result=result)
            task.release_payload()
            task = None  # drop queue-entry reference promptly
