"""threadpool - 固定线程数 + 有界优先级队列线程池（仅 Python 标准库）。

特性：
- 固定工作线程数，有界任务队列，按优先级出队（数值小者先出队，同级 FIFO）。
- 队列满时可选择阻塞（可带超时）或立即拒绝，拒绝抛出带明确错误码的 QueueFull。
- 优雅关闭 shutdown()：停止接收新任务，在途与已排队任务执行完后退出。
- 立即中止 abort()：丢弃未开始的任务并标记为 ABORTED（可识别），正在运行的
  任务自然结束（Python 无法安全地杀死线程）。
- 任务内抛出的异常不会带走工作线程，异常通过 Future 回传给提交方。
- 池内任务互相等待：工作线程等待本池尚未开始的任务时会"内联执行"该任务，
  避免线程耗尽型死锁；result(timeout) 提供超时兜底，超时后互等被释放。
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
    "QueueFull",
    "PoolShutdown",
    "TaskCancelled",
    "TaskAborted",
    "TaskTimeout",
    "Future",
    "ThreadPool",
]


class ErrorCode(Enum):
    OK = 0
    QUEUE_FULL = 1
    SHUTTING_DOWN = 2
    CANCELLED = 3
    ABORTED = 4
    TIMEOUT = 5
    TASK_FAILED = 6


class PoolError(Exception):
    """所有池相关错误的基类，携带明确的错误码。"""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code


class QueueFull(PoolError):
    def __init__(self, message: str = "task queue is full"):
        super().__init__(ErrorCode.QUEUE_FULL, message)


class PoolShutdown(PoolError):
    def __init__(self, message: str = "pool is shutting down"):
        super().__init__(ErrorCode.SHUTTING_DOWN, message)


class TaskCancelled(PoolError):
    def __init__(self, message: str = "task was cancelled before it started"):
        super().__init__(ErrorCode.CANCELLED, message)


class TaskAborted(PoolError):
    def __init__(self, message: str = "task was aborted by pool abort()"):
        super().__init__(ErrorCode.ABORTED, message)


class TaskTimeout(PoolError):
    def __init__(self, message: str = "timed out waiting for task result"):
        super().__init__(ErrorCode.TIMEOUT, message)


# Future 状态
_PENDING = 0
_RUNNING = 1
_CANCELLED = 2
_ABORTED = 3
_FINISHED = 4
_FAILED = 5

_STATE_NAMES = {
    _PENDING: "PENDING",
    _RUNNING: "RUNNING",
    _CANCELLED: "CANCELLED",
    _ABORTED: "ABORTED",
    _FINISHED: "FINISHED",
    _FAILED: "FAILED",
}


class _Task:
    __slots__ = ("fn", "args", "kwargs", "future", "state")

    def __init__(self, fn, args, kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.future = None
        self.state = _PENDING


class Future:
    """submit() 的返回句柄，用于取结果、异常、取消与状态查询。"""

    def __init__(self, pool, task):
        self._cond = threading.Condition()
        self._state = _PENDING
        self._result = None
        self._exception = None
        self._pool = pool
        self._task = task
        self._callbacks = []

    # ---- 状态查询 ----
    @property
    def state(self):
        return _STATE_NAMES[self._state]

    def done(self):
        return self._state in (_CANCELLED, _ABORTED, _FINISHED, _FAILED)

    def cancelled(self):
        return self._state == _CANCELLED

    def aborted(self):
        return self._state == _ABORTED

    def running(self):
        return self._state == _RUNNING

    # ---- 取消 ----
    def cancel(self):
        """任务尚未开始时取消；已开始运行则返回 False。"""
        pool = self._pool
        with self._cond:
            if self._state != _PENDING:
                return False
            if pool is not None and self._task is not None:
                with pool._lock:
                    if self._task.state != _PENDING:
                        return False
                    self._task.state = _CANCELLED
            self._state = _CANCELLED
            self._exception = TaskCancelled()
            self._cond.notify_all()
        self._run_callbacks()
        return True

    # ---- 取结果 ----
    def result(self, timeout=None):
        """阻塞直到完成并返回结果。

        - 任务失败：重新抛出任务内的异常；
        - 被取消/中止：抛出 TaskCancelled / TaskAborted；
        - timeout 到期仍未完成：抛出 TaskTimeout（code=TIMEOUT）。
        若调用线程是本池工作线程且任务尚未开始，则内联执行该任务以避免互等死锁。
        """
        self._maybe_inline()
        with self._cond:
            return self._wait_locked(timeout, want="result")

    def exception(self, timeout=None):
        """阻塞直到完成并返回异常对象（无异常返回 None）；超时抛 TaskTimeout。"""
        self._maybe_inline()
        with self._cond:
            self._wait_locked(timeout, want="exception")
            return self._exception

    def _wait_locked(self, timeout, want):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self._state in (_PENDING, _RUNNING):
            if deadline is None:
                self._cond.wait()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TaskTimeout()
                self._cond.wait(remaining)
        if self._state == _CANCELLED:
            raise TaskCancelled()
        if self._state == _ABORTED:
            raise TaskAborted()
        if want == "result":
            if self._state == _FAILED:
                raise self._exception
            return self._result
        return None

    # ---- 回调 ----
    def add_done_callback(self, fn):
        with self._cond:
            if self.done():
                run_now = True
            else:
                run_now = False
                self._callbacks.append(fn)
        if run_now:
            fn(self)

    # ---- 内部：由池调用 ----
    def _set_running(self):
        with self._cond:
            if self._state == _PENDING:
                self._state = _RUNNING

    def _set_result(self, value):
        with self._cond:
            if self._state in (_CANCELLED, _ABORTED):
                return
            self._state = _FINISHED
            self._result = value
            self._task = None  # 断开引用，便于 GC
            self._cond.notify_all()
        self._run_callbacks()

    def _set_exception(self, exc, state=_FAILED):
        with self._cond:
            if self._state in (_CANCELLED, _ABORTED):
                return
            self._state = state
            self._exception = exc
            self._task = None
            self._cond.notify_all()
        self._run_callbacks()

    def _run_callbacks(self):
        callbacks, self._callbacks = self._callbacks, []
        for fn in callbacks:
            try:
                fn(self)
            except Exception:
                pass

    def _maybe_inline(self):
        """工作线程等待本池 PENDING 任务时，尝试认领并内联执行（防互等死锁）。"""
        pool = self._pool
        task = self._task
        if pool is None or task is None or self._state != _PENDING:
            return
        if not pool._is_worker_thread():
            return
        if pool._claim(task):
            pool._execute(task)


class ThreadPool:
    """固定线程数、有界优先级队列的线程池。

    :param max_workers: 工作线程数（>=1）。
    :param maxsize: 队列容量上限；None 表示无界（默认 1024）。
    :param thread_name_prefix: 工作线程名前缀。
    """

    def __init__(self, max_workers, maxsize=1024, thread_name_prefix="threadpool"):
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        if maxsize is not None and maxsize < 1:
            raise ValueError("maxsize must be >= 1 or None")
        self._max_workers = max_workers
        self._maxsize = maxsize
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._queue = []  # 堆元素: (priority, seq, _Task)
        self._counter = itertools.count()
        self._shutdown = False
        self._aborted = False
        self._worker_idents = set()
        self._threads = []
        for i in range(max_workers):
            t = threading.Thread(
                target=self._worker,
                name="%s-%d" % (thread_name_prefix, i),
                daemon=True,
            )
            self._threads.append(t)
            t.start()

    # ---- 提交 ----
    def submit(self, fn, *args, priority=0, block=True, timeout=None, **kwargs):
        """提交任务，返回 Future。

        :param priority: 数值越小越先出队，同级按提交顺序（FIFO）。
        :param block: 队列满时是否阻塞等待空位；False 则立即抛 QueueFull。
        :param timeout: 阻塞等待空位的最长时间（秒），超时抛 QueueFull。
        :raises PoolShutdown: 池已关闭（code=SHUTTING_DOWN）。
        :raises QueueFull: 队列满且拒绝/超时（code=QUEUE_FULL）。
        """
        task = _Task(fn, args, kwargs)
        future = Future(self, task)
        task.future = future
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while True:
                if self._shutdown:
                    raise PoolShutdown()
                if self._maxsize is None or len(self._queue) < self._maxsize:
                    heapq.heappush(self._queue,
                                   (priority, next(self._counter), task))
                    self._not_empty.notify()
                    return future
                if not block:
                    raise QueueFull()
                if deadline is None:
                    self._not_full.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise QueueFull("timed out waiting for queue space")
                    self._not_full.wait(remaining)

    # ---- 关闭 ----
    def shutdown(self, wait=True):
        """优雅关闭：停止接收新任务，已提交的任务全部执行完后线程退出。

        可重复调用（幂等）。wait=True 时阻塞直到所有工作线程退出。
        """
        with self._lock:
            if not self._shutdown:
                self._shutdown = True
                self._not_empty.notify_all()
                self._not_full.notify_all()  # 唤醒阻塞中的提交者，使其收到 PoolShutdown
        if wait:
            self._join_workers()

    def abort(self):
        """立即中止：丢弃所有未开始的任务（标记为 ABORTED），并等待线程退出。

        正在运行的任务无法被强制杀死，会自然运行结束；被丢弃任务的 Future
        处于 ABORTED 状态，result() 抛出 TaskAborted（code=ABORTED），可明确识别。
        幂等；调用后池即视为已关闭。
        """
        abandoned = []
        with self._lock:
            if not self._aborted:
                self._aborted = True
                self._shutdown = True
                queue, self._queue = self._queue, []
                for _, _, task in queue:
                    if task.state == _PENDING:
                        task.state = _ABORTED
                        abandoned.append(task.future)
                self._not_empty.notify_all()
                self._not_full.notify_all()
        for future in abandoned:
            future._set_exception(TaskAborted(), state=_ABORTED)
        self._join_workers()

    def _join_workers(self):
        current = threading.current_thread()
        for t in list(self._threads):
            if t is not current and t.is_alive():
                t.join()
        # 线程与队列引用释放
        with self._lock:
            self._threads = [t for t in self._threads if t.is_alive()]
            self._worker_idents = {t.ident for t in self._threads}

    # ---- 工作线程 ----
    def _worker(self):
        self._worker_idents.add(threading.get_ident())
        try:
            while True:
                with self._lock:
                    while not self._queue and not self._shutdown:
                        self._not_empty.wait()
                    if self._aborted or (self._shutdown and not self._queue):
                        return
                    _, _, task = heapq.heappop(self._queue)
                    self._not_full.notify()
                    if task.state != _PENDING:
                        continue  # 已被取消/中止/内联认领，跳过
                    task.state = _RUNNING
                self._execute(task)
        finally:
            self._worker_idents.discard(threading.get_ident())

    def _execute(self, task):
        future = task.future
        future._set_running()
        try:
            result = task.fn(*task.args, **task.kwargs)
        except BaseException as exc:  # 异常不带走工作线程，回传提交方
            future._set_exception(exc)
        else:
            future._set_result(result)
        finally:
            # 释放任务持有的引用，避免池/队列长期占用内存
            task.fn = task.args = task.kwargs = None
            task.future = None

    # ---- 内联认领（防池内互等死锁）----
    def _is_worker_thread(self):
        return threading.get_ident() in self._worker_idents

    def _claim(self, task):
        with self._lock:
            if task.state != _PENDING:
                return False
            task.state = _RUNNING
            return True

    # ---- 状态 ----
    @property
    def is_shutdown(self):
        return self._shutdown

    @property
    def queue_size(self):
        with self._lock:
            return len(self._queue)

    @property
    def alive_workers(self):
        return sum(1 for t in self._threads if t.is_alive())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.shutdown(wait=True)
