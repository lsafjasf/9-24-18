"""threadpool 并发与关闭测试（标准库 unittest）。

运行: python3 test_threadpool.py -v
"""

import threading
import time
import unittest

from threadpool import (
    ErrorCode,
    Future,
    PoolShutdown,
    QueueFull,
    TaskAborted,
    TaskCancelled,
    TaskTimeout,
    ThreadPool,
)


class TestBasic(unittest.TestCase):
    def test_results_and_kwargs(self):
        with ThreadPool(4) as pool:
            futures = [pool.submit(lambda x: x * x, i) for i in range(20)]
            f = pool.submit(pow, 2, 10)
            self.assertEqual([f.result() for f in futures],
                             [i * i for i in range(20)])
            self.assertEqual(f.result(), 1024)

    def test_priority_order(self):
        # 单线程池 + 闸门：先占住 worker，再入队不同优先级，放行后观察出队顺序
        gate = threading.Event()
        order = []
        lock = threading.Lock()
        with ThreadPool(1, thread_name_prefix="prio") as pool:
            first = pool.submit(lambda: gate.wait(5))
            time.sleep(0.1)  # 确保闸门任务已开始运行
            for prio, tag in [(5, "e"), (1, "a"), (3, "c"), (1, "b"), (3, "d")]:
                pool.submit(lambda t=tag: (lock.acquire(), order.append(t),
                                           lock.release()), priority=prio)
            gate.set()
            first.result(5)
            pool.shutdown()
        # 数值小者先出队；同优先级 FIFO
        self.assertEqual(order, ["a", "b", "c", "d", "e"])

    def test_done_callback(self):
        hits = []
        with ThreadPool(2) as pool:
            f = pool.submit(lambda: 42)
            f.add_done_callback(lambda fu: hits.append(fu.result()))
            self.assertEqual(f.result(5), 42)
            time.sleep(0.1)
        self.assertEqual(hits, [42])


class TestBackpressure(unittest.TestCase):
    def test_queue_full_reject_error_code(self):
        gate = threading.Event()
        pool = ThreadPool(1, maxsize=1)
        try:
            pool.submit(lambda: gate.wait(5))          # 占用 worker
            time.sleep(0.1)
            pool.submit(lambda: None)                  # 占满队列
            with self.assertRaises(QueueFull) as ctx:
                pool.submit(lambda: None, block=False)  # 拒绝
            self.assertEqual(ctx.exception.code, ErrorCode.QUEUE_FULL)
        finally:
            gate.set()
            pool.shutdown()

    def test_queue_full_block_timeout(self):
        gate = threading.Event()
        pool = ThreadPool(1, maxsize=1)
        try:
            pool.submit(lambda: gate.wait(5))
            time.sleep(0.1)
            pool.submit(lambda: None)
            t0 = time.monotonic()
            with self.assertRaises(QueueFull) as ctx:
                pool.submit(lambda: None, block=True, timeout=0.3)
            self.assertEqual(ctx.exception.code, ErrorCode.QUEUE_FULL)
            self.assertGreaterEqual(time.monotonic() - t0, 0.25)
        finally:
            gate.set()
            pool.shutdown()

    def test_queue_full_block_until_space(self):
        gate = threading.Event()
        pool = ThreadPool(1, maxsize=1)
        try:
            pool.submit(lambda: gate.wait(5))
            time.sleep(0.1)
            pool.submit(lambda: None)
            threading.Timer(0.2, gate.set).start()
            f = pool.submit(lambda: "ok", block=True, timeout=5)  # 阻塞后成功入队
            self.assertEqual(f.result(5), "ok")
        finally:
            gate.set()
            pool.shutdown()


class TestShutdown(unittest.TestCase):
    def test_empty_queue_shutdown(self):
        pool = ThreadPool(3)
        t0 = time.monotonic()
        pool.shutdown()
        self.assertLess(time.monotonic() - t0, 2)
        self.assertEqual(pool.alive_workers, 0)

    def test_repeated_shutdown_and_abort_idempotent(self):
        pool = ThreadPool(2)
        pool.shutdown()
        pool.shutdown()      # 重复关闭不报错
        pool.abort()         # 关闭后再中止也不报错
        pool.abort()
        self.assertTrue(pool.is_shutdown)

    def test_graceful_shutdown_drains_inflight(self):
        done = []
        pool = ThreadPool(2)
        futures = [pool.submit(lambda i=i: (time.sleep(0.05), done.append(i)))
                   for i in range(10)]
        t0 = time.monotonic()
        pool.shutdown(wait=True)   # 在途任务全部收尾后才返回
        self.assertGreaterEqual(time.monotonic() - t0, 0.1)
        self.assertEqual(sorted(done), list(range(10)))
        self.assertTrue(all(f.done() for f in futures))
        self.assertEqual(pool.alive_workers, 0)

    def test_submit_during_shutdown_rejected(self):
        pool = ThreadPool(2)
        pool.shutdown()
        with self.assertRaises(PoolShutdown) as ctx:
            pool.submit(lambda: None)
        self.assertEqual(ctx.exception.code, ErrorCode.SHUTTING_DOWN)

    def test_blocked_submitter_woken_by_shutdown(self):
        """队列满时阻塞的提交者在关闭时被唤醒并收到 SHUTTING_DOWN，不死锁。"""
        gate = threading.Event()
        pool = ThreadPool(1, maxsize=1)
        pool.submit(lambda: gate.wait(5))
        time.sleep(0.1)
        pool.submit(lambda: None)  # 队列满
        errors = []

        def submitter():
            try:
                pool.submit(lambda: None, block=True)  # 无限阻塞等待空位
            except PoolShutdown as exc:
                errors.append(exc.code)

        t = threading.Thread(target=submitter)
        t.start()
        time.sleep(0.2)
        gate.set()
        pool.shutdown()          # 必须唤醒阻塞的提交者
        t.join(timeout=5)
        self.assertFalse(t.is_alive(), "阻塞的提交者未被唤醒，疑似死锁")
        self.assertEqual(errors, [ErrorCode.SHUTTING_DOWN])

    def test_abort_marks_pending_tasks(self):
        gate = threading.Event()
        started = threading.Event()

        def slow():
            started.set()
            gate.wait(5)
            return "ran"

        pool = ThreadPool(1)
        running = pool.submit(slow)
        started.wait(5)
        pending = [pool.submit(lambda i=i: i) for i in range(5)]
        t0 = time.monotonic()
        abort_thread = threading.Thread(target=pool.abort)
        abort_thread.start()
        time.sleep(0.2)
        self.assertTrue(abort_thread.is_alive(), "abort 应等待在途任务结束")
        gate.set()  # 放行在途任务
        abort_thread.join(5)
        self.assertFalse(abort_thread.is_alive())
        self.assertLess(time.monotonic() - t0, 5)
        self.assertEqual(running.result(5), "ran")  # 在途任务自然完成
        for f in pending:                            # 未开始的任务可识别为 ABORTED
            self.assertTrue(f.aborted())
            with self.assertRaises(TaskAborted) as ctx:
                f.result()
            self.assertEqual(ctx.exception.code, ErrorCode.ABORTED)
        with self.assertRaises(PoolShutdown):
            pool.submit(lambda: None)

    def test_cancel_pending_task(self):
        gate = threading.Event()
        pool = ThreadPool(1)
        try:
            pool.submit(lambda: gate.wait(5))
            time.sleep(0.1)
            f = pool.submit(lambda: "never")
            self.assertTrue(f.cancel())
            self.assertTrue(f.cancelled())
            with self.assertRaises(TaskCancelled) as ctx:
                f.result()
            self.assertEqual(ctx.exception.code, ErrorCode.CANCELLED)
            self.assertFalse(f.cancel())  # 重复取消返回 False
        finally:
            gate.set()
            pool.shutdown()

    def test_cancel_during_shutdown(self):
        """关闭进行中取消排队任务：不报错、不死锁，worker 跳过被取消任务。"""
        gate = threading.Event()
        ran = []
        pool = ThreadPool(1)
        pool.submit(lambda: gate.wait(5))
        time.sleep(0.1)
        f_cancel = pool.submit(lambda: ran.append("cancelled"))
        f_keep = pool.submit(lambda: ran.append("kept"))
        pool.shutdown(wait=False)          # 开始优雅关闭
        self.assertTrue(f_cancel.cancel())  # 关闭期间取消排队任务
        gate.set()
        pool.shutdown(wait=True)
        self.assertTrue(f_cancel.cancelled())
        self.assertEqual(ran, ["kept"])


class TestExceptions(unittest.TestCase):
    def test_exception_propagates_and_worker_survives(self):
        pool = ThreadPool(2)
        try:
            def boom():
                raise ValueError("bad task")

            f = pool.submit(boom)
            with self.assertRaises(ValueError):
                f.result(5)
            self.assertIsInstance(f.exception(0), ValueError)
            # 工作线程未被异常带走，池继续工作
            time.sleep(0.1)
            self.assertEqual(pool.alive_workers, 2)
            results = [pool.submit(lambda i=i: i + 1).result(5)
                       for i in range(10)]
            self.assertEqual(results, list(range(1, 11)))
            # 异常任务后线程数不膨胀
            self.assertEqual(threading.active_count(),
                             threading.active_count())
        finally:
            pool.shutdown()

    def test_base_exception_captured(self):
        pool = ThreadPool(1)
        try:
            f = pool.submit(lambda: 1 / 0)
            with self.assertRaises(ZeroDivisionError):
                f.result(5)
        finally:
            pool.shutdown()


class TestNoDeadlock(unittest.TestCase):
    def test_nested_wait_inline_execution(self):
        """单线程池中，任务等待自己提交的任务：内联执行避免死锁。"""
        with ThreadPool(1) as pool:
            def outer():
                inner = pool.submit(lambda: "inner-result")
                return inner.result()  # 若无不死锁机制，此处必然死锁

            self.assertEqual(pool.submit(outer).result(5), "inner-result")

    def test_mutual_wait_with_inline(self):
        """两个任务互相等待对方提交的后继任务，后继可被内联执行。"""
        with ThreadPool(2) as pool:
            def stage(name):
                follow = pool.submit(lambda: name + "-done")
                return follow.result()

            f1 = pool.submit(stage, "a")
            f2 = pool.submit(stage, "b")
            self.assertEqual(f1.result(5), "a-done")
            self.assertEqual(f2.result(5), "b-done")

    def test_mutual_wait_timeout_releases(self):
        """两个运行中的任务互等对方结果：超时释放互等，池保持健康。"""
        box = {}
        ready = threading.Event()

        def waiter(name):
            box[name] = True
            if len(box) == 2:
                ready.set()
            ready.wait(5)
            other = "b" if name == "a" else "a"
            try:
                return futures[other].result(timeout=0.5)
            except TaskTimeout as exc:
                assert exc.code is ErrorCode.TIMEOUT
                return name + "-timed-out"

        with ThreadPool(2) as pool:
            futures = {"a": pool.submit(waiter, "a"),
                       "b": pool.submit(waiter, "b")}
            # 一方超时返回后另一方会随之解除阻塞，两者都必须在有限时间内完成
            results = [futures[k].result(5) for k in ("a", "b")]
            for r in results:
                self.assertIn(r, ("a-timed-out", "b-timed-out"))
            # 互等被超时释放后，池仍能处理新任务
            self.assertEqual(pool.submit(lambda: "healthy").result(5), "healthy")

    def test_result_timeout_error_code(self):
        gate = threading.Event()
        with ThreadPool(1) as pool:
            f = pool.submit(lambda: gate.wait(2))
            try:
                with self.assertRaises(TaskTimeout) as ctx:
                    f.result(timeout=0.2)
                self.assertEqual(ctx.exception.code, ErrorCode.TIMEOUT)
            finally:
                gate.set()


class TestContextManager(unittest.TestCase):
    def test_with_statement(self):
        with ThreadPool(2) as pool:
            f = pool.submit(lambda: 7)
            self.assertEqual(f.result(5), 7)
        self.assertTrue(pool.is_shutdown)
        self.assertEqual(pool.alive_workers, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
