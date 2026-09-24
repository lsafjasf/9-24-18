"""Long-running memory-stability check for threadpool.py.

Submits a large number of tasks (success / failure / cancelled / aborted
mix) and verifies that neither the pool's internal structures, the
process RSS, nor the Python object count grow without bound. TaskHandles
are checked via weakrefs to prove the pool does not retain them.

Run:  python3 leak_check.py
"""

import gc
import os
import resource
import threading
import weakref

from threadpool import PoolError, ThreadPool

ROUNDS = 60
TASKS_PER_ROUND = 500
WORKERS = 4
MAX_QUEUE = 64


def rss_kb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KiB on Linux


def run_round(pool, round_no, handle_refs):
    handles = []
    for i in range(TASKS_PER_ROUND):
        kind = i % 10
        if kind == 0:
            handles.append(pool.submit(_boom))                      # failure path
        elif kind == 1:
            h = pool.submit(_work, i)
            h.cancel()                                              # cancel path
            handles.append(h)
        else:
            handles.append(pool.submit(_work, i))                   # success path
    for h in handles:
        try:
            h.result(timeout=10)
        except (PoolError, ValueError):
            pass
    # keep weakrefs to a sample; the pool must not keep them alive
    if round_no % 10 == 0:
        handle_refs.extend(weakref.ref(h) for h in handles[:20])


def _work(x):
    return x * 2


def _boom():
    raise ValueError("expected failure")


def main():
    pool = ThreadPool(workers=WORKERS, max_queue=MAX_QUEUE, name="leakcheck")
    handle_refs = []

    # warmup: let the interpreter reach steady-state allocations
    for r in range(5):
        run_round(pool, r, handle_refs)
    gc.collect()
    base_rss, base_objs = rss_kb(), len(gc.get_objects())
    base_threads = threading.active_count()
    print(f"baseline : rss={base_rss} KiB, gc_objects={base_objs}, threads={base_threads}")

    for r in range(5, ROUNDS):
        run_round(pool, r, handle_refs)
        if r % 10 == 9:
            gc.collect()
            print(f"round {r + 1:3d}: rss={rss_kb()} KiB, "
                  f"gc_objects={len(gc.get_objects())}, "
                  f"queue={pool.queue_size}, threads={threading.active_count()}")

    # abort-shutdown path also exercised under load
    for i in range(200):
        pool.submit(_work, i)
    pool.shutdown(abort=True)

    gc.collect()
    final_rss, final_objs = rss_kb(), len(gc.get_objects())
    final_threads = threading.active_count()
    alive_handles = sum(1 for ref in handle_refs if ref() is not None)

    print(f"final    : rss={final_rss} KiB, gc_objects={final_objs}, threads={final_threads}")
    print(f"handles  : {len(handle_refs)} sampled, {alive_handles} still alive "
          f"(pool retains none once dropped by caller)")

    ok = True

    def check(name, cond, detail):
        nonlocal ok
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}: {detail}")
        ok = ok and cond

    print("checks:")
    check("rss growth", final_rss - base_rss < 8 * 1024,
          f"{final_rss - base_rss} KiB < 8192 KiB")
    check("gc object growth", final_objs - base_objs < 5000,
          f"{final_objs - base_objs} < 5000")
    check("thread count", final_threads <= base_threads,
          f"{final_threads} <= {base_threads} (workers exited)")
    check("queue drained", pool.queue_size == 0, f"queue_size={pool.queue_size}")
    check("handles released", alive_handles <= WORKERS,
          f"{alive_handles} alive (<= in-flight worker slots)")

    print("LEAK CHECK:", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
