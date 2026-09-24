"""Concurrency and shutdown tests for threadpool.py (stdlib unittest)."""

import threading
import time
import unittest

from threadpool import ErrorCode, PoolError, TaskState, ThreadPool


def make_pool(workers=2, max_queue=8):
    return ThreadPool(workers=workers, max_queue=max_queue, name="test")


class BasicBehaviour(unittest.TestCase):
    def test_result_and_parallelism(self):
        pool = make_pool(workers=4)
        try:
            handles = [pool.submit(lambda x: x * x, i) for i in range(20)]
            self.assertEqual([h.result() for h in handles], [i * i for i in range(20)])
        finally:
            pool.shutdown()

    def test_priority_order(self):
        # One worker + a blocker so queued tasks pile up, then check order.
        pool = make_pool(workers=1, max_queue=16)
        gate = threading.Event()
        first = pool.submit(gate.wait)
        time.sleep(0.05)  # ensure blocker is running
        order = []
        pool.submit(lambda: order.append("low"), priority=10)
        pool.submit(lambda: order.append("high"), priority=1)
        pool.submit(lambda: order.append("mid"), priority=5)
        pool.submit(lambda: order.append("high2"), priority=1)  # FIFO tie-break
        gate.set()
        first.result()
        pool.shutdown()
        self.assertEqual(order, ["high", "high2", "mid", "low"])

    def test_exception_propagates_and_worker_survives(self):
        pool = make_pool(workers=1)
        try:
            def boom():
                raise ValueError("kaboom")

            h = pool.submit(boom)
            with self.assertRaises(ValueError):
                h.result()
            self.assertIsInstance(h.exception(), ValueError)
            self.assertEqual(h.state, TaskState.FAILED)
            # worker thread still healthy
            self.assertEqual(pool.submit(lambda: 42).result(), 42)
        finally:
            pool.shutdown()


class Backpressure(unittest.TestCase):
    def test_queue_full_reject(self):
        pool = make_pool(workers=1, max_queue=2)
        gate = threading.Event()
        pool.submit(gate.wait)  # occupies the only worker
        time.sleep(0.05)
        pool.submit(lambda: None)
        pool.submit(lambda: None)  # queue now full (2/2)
        try:
            with self.assertRaises(PoolError) as ctx:
                pool.submit(lambda: None, block=False)
            self.assertEqual(ctx.exception.code, ErrorCode.QUEUE_FULL)
        finally:
            gate.set()
            pool.shutdown()

    def test_queue_full_block_then_succeed(self):
        pool = make_pool(workers=1, max_queue=1)
        gate = threading.Event()
        pool.submit(gate.wait)
        time.sleep(0.05)
        pool.submit(lambda: None)  # queue full
        gate.set()  # worker will free a slot shortly
        try:
            h = pool.submit(lambda: "made-it", block=True, timeout=5)
            self.assertEqual(h.result(), "made-it")
        finally:
            pool.shutdown()

    def test_queue_full_block_timeout(self):
        pool = make_pool(workers=1, max_queue=1)
        gate = threading.Event()
        pool.submit(gate.wait)
        time.sleep(0.05)
        pool.submit(lambda: None)  # full
        try:
            t0 = time.monotonic()
            with self.assertRaises(PoolError) as ctx:
                pool.submit(lambda: None, block=True, timeout=0.2)
            self.assertEqual(ctx.exception.code, ErrorCode.QUEUE_FULL)
            self.assertLess(time.monotonic() - t0, 2.0)
        finally:
            gate.set()
            pool.shutdown()

    def test_blocked_submitter_woken_by_shutdown(self):
        """A submitter blocked on a full queue must not deadlock on shutdown."""
        pool = make_pool(workers=1, max_queue=1)
        gate = threading.Event()
        pool.submit(gate.wait)
        time.sleep(0.05)
        pool.submit(lambda: None)  # full
        errors = []

        def blocked_submit():
            try:
                pool.submit(lambda: None, block=True)  # no timeout
            except PoolError as exc:
                errors.append(exc.code)

        t = threading.Thread(target=blocked_submit)
        t.start()
        time.sleep(0.1)  # let it block on the full queue
        gate.set()
        pool.shutdown()  # must wake the blocked submitter
        t.join(timeout=5)
        self.assertFalse(t.is_alive(), "blocked submitter deadlocked")
        self.assertEqual(errors, [ErrorCode.POOL_SHUTDOWN])


class Shutdown(unittest.TestCase):
    def test_empty_queue_shutdown(self):
        pool = make_pool(workers=3)
        t0 = time.monotonic()
        pool.shutdown()
        self.assertLess(time.monotonic() - t0, 2.0)
        self.assertTrue(pool.is_shutdown)

    def test_repeated_shutdown_is_safe(self):
        pool = make_pool()
        pool.shutdown()
        pool.shutdown()          # no-op
        pool.shutdown(abort=True)  # still safe after workers exited
        pool.join(timeout=2)

    def test_shutdown_escalates_to_abort(self):
        pool = make_pool(workers=1, max_queue=8)
        gate = threading.Event()
        pool.submit(gate.wait)
        time.sleep(0.05)
        pending = [pool.submit(lambda: i) for i in range(4)]
        pool.shutdown(wait=False)            # graceful: would drain
        pool.shutdown(wait=False, abort=True)  # escalate: drop pending
        gate.set()
        pool.join(timeout=5)
        for h in pending:
            self.assertEqual(h.state, TaskState.ABORTED)

    def test_graceful_shutdown_drains_inflight(self):
        pool = make_pool(workers=2, max_queue=8)
        done = []
        handles = [pool.submit(lambda i: (time.sleep(0.02), done.append(i)), i)
                   for i in range(6)]
        pool.shutdown()  # returns only after all 6 finished
        self.assertEqual(sorted(done), list(range(6)))
        for h in handles:
            self.assertEqual(h.state, TaskState.SUCCEEDED)

    def test_submit_after_shutdown_rejected(self):
        pool = make_pool()
        pool.shutdown()
        with self.assertRaises(PoolError) as ctx:
            pool.submit(lambda: None)
        self.assertEqual(ctx.exception.code, ErrorCode.POOL_SHUTDOWN)

    def test_submit_during_shutdown_rejected(self):
        pool = make_pool(workers=1)
        gate = threading.Event()
        pool.submit(gate.wait)
        time.sleep(0.05)
        pool.shutdown(wait=False)
        try:
            with self.assertRaises(PoolError) as ctx:
                pool.submit(lambda: None)
            self.assertEqual(ctx.exception.code, ErrorCode.POOL_SHUTDOWN)
        finally:
            gate.set()
            pool.join(timeout=5)

    def test_abort_marks_pending_tasks_identifiable(self):
        pool = make_pool(workers=1, max_queue=8)
        gate = threading.Event()
        running = pool.submit(gate.wait)
        time.sleep(0.05)
        pending = [pool.submit(lambda: i) for i in range(3)]
        pool.shutdown(wait=False, abort=True)
        for h in pending:
            self.assertEqual(h.state, TaskState.ABORTED)
            with self.assertRaises(PoolError) as ctx:
                h.result()
            self.assertEqual(ctx.exception.code, ErrorCode.TASK_ABORTED)
        gate.set()
        self.assertTrue(running.result())  # running task still completes
        pool.join(timeout=5)

    def test_cancel_during_shutdown(self):
        pool = make_pool(workers=1, max_queue=8)
        gate = threading.Event()
        pool.submit(gate.wait)
        time.sleep(0.05)
        victim = pool.submit(lambda: "never")
        survivor = pool.submit(lambda: "ran")
        pool.shutdown(wait=False)  # graceful shutdown in progress
        self.assertTrue(victim.cancel())
        self.assertFalse(victim.cancel())  # idempotent-ish: already cancelled
        gate.set()
        pool.join(timeout=5)
        self.assertEqual(victim.state, TaskState.CANCELLED)
        with self.assertRaises(PoolError) as ctx:
            victim.result()
        self.assertEqual(ctx.exception.code, ErrorCode.TASK_CANCELLED)
        self.assertEqual(survivor.result(), "ran")

    def test_cancel_running_task_fails(self):
        pool = make_pool(workers=1)
        gate = threading.Event()
        h = pool.submit(gate.wait)
        time.sleep(0.05)
        self.assertFalse(h.cancel())  # already running
        gate.set()
        h.result()
        self.assertFalse(h.cancel())  # already finished
        pool.shutdown()


class MutualWait(unittest.TestCase):
    def test_mutual_wait_with_timeout_unwinds(self):
        """Two pool tasks waiting on each other must not deadlock the pool:
        bounded waits time out, release the workers, and the pool recovers."""
        pool = make_pool(workers=2, max_queue=8)
        handles = {}

        def waiter(name, timeout):
            other = handles["b" if name == "a" else "a"]
            return other.result(timeout=timeout)

        handles["a"] = pool.submit(waiter, "a", 0.3)
        handles["b"] = pool.submit(waiter, "b", 0.3)
        for name in ("a", "b"):
            with self.assertRaises(PoolError) as ctx:
                handles[name].result(timeout=5)
            self.assertEqual(ctx.exception.code, ErrorCode.TASK_TIMEOUT)
        # pool fully recovered and usable
        self.assertEqual(pool.submit(lambda: "alive").result(timeout=5), "alive")
        pool.shutdown()

    def test_dependent_chain_completes(self):
        """Non-cyclic intra-pool dependency (submitter outside the pool)
        completes normally."""
        pool = make_pool(workers=2)
        try:
            a = pool.submit(lambda: 1)
            b = pool.submit(lambda: a.result() + 1)
            c = pool.submit(lambda: b.result() + 1)
            self.assertEqual(c.result(timeout=5), 3)
        finally:
            pool.shutdown()


class ThreadLifecycle(unittest.TestCase):
    def test_workers_exit_and_release(self):
        pool = make_pool(workers=3)
        names = [t.name for t in pool._threads]
        pool.submit(lambda: None).result()
        pool.shutdown()
        self.assertEqual(pool._threads, [])  # Thread objects released
        alive = [t for t in threading.enumerate() if t.name in names]
        self.assertEqual(alive, [])

    def test_context_manager(self):
        with make_pool() as pool:
            self.assertEqual(pool.submit(lambda: 7).result(), 7)
        self.assertTrue(pool.is_shutdown)


if __name__ == "__main__":
    unittest.main(verbosity=2)
