"""leak_check - 线程池长期运行的内存/线程泄漏检查。

反复创建池、提交大量任务（含异常任务与大返回值）、关闭并丢弃全部引用，
采样 RSS / GC 对象数 / 存活线程数，验证三者均进入平台期而不持续增长。

运行: python3 leak_check.py [--cycles 300] [--workers 4] [--tasks 500]
"""

import argparse
import gc
import threading

from threadpool import ThreadPool

try:  # 让 glibc 把空闲 arena 还给内核，消除采样抖动（非 Linux 时跳过）
    import ctypes
    _malloc_trim = ctypes.CDLL("libc.so.6").malloc_trim
except (OSError, AttributeError):
    _malloc_trim = None


def rss_mb():
    """当前常驻内存（Linux /proc），不可用时退化为 ru_maxrss 高水位。"""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def one_cycle(workers, tasks):
    pool = ThreadPool(workers, maxsize=64, thread_name_prefix="leakcheck")
    futures = []
    for i in range(tasks):
        if i % 17 == 0:
            def boom():
                raise RuntimeError("expected failure")
            futures.append(pool.submit(boom, block=True, timeout=10))
        else:
            futures.append(pool.submit(lambda x: bytes(x % 64) * 1024,
                                       i, block=True, timeout=10))
    for f in futures:
        try:
            f.result(10)
        except RuntimeError:
            pass
    pool.shutdown(wait=True)
    if pool.alive_workers != 0:
        raise AssertionError("worker threads still alive after shutdown")
    # 丢弃全部引用，下一行之后池/队列/Future 都应可回收
    del futures, pool


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycles", type=int, default=300)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--tasks", type=int, default=500)
    ap.add_argument("--warmup", type=int, default=20)
    ap.add_argument("--budget-mb", type=float, default=8.0,
                    help="热身后允许的 RSS 增长上限")
    args = ap.parse_args()

    base_threads = threading.active_count()
    samples = []
    print("%6s %10s %10s %8s" % ("cycle", "RSS(MB)", "gc_objs", "threads"))
    for i in range(1, args.cycles + 1):
        one_cycle(args.workers, args.tasks)
        if threading.active_count() != base_threads:
            raise AssertionError(
                "thread leak: active=%d base=%d" %
                (threading.active_count(), base_threads))
        if i % args.warmup == 0 or i == args.cycles:
            gc.collect()
            if _malloc_trim is not None:
                _malloc_trim(0)
            rss, objs = rss_mb(), len(gc.get_objects())
            samples.append((i, rss, objs))
            print("%6d %10.1f %10d %8d" % (i, rss, objs,
                                           threading.active_count()))

    head = sorted(s[1] for s in samples[:3])
    tail = sorted(s[1] for s in samples[-3:])
    growth = tail[1] - head[1]  # 中位数对中位数，抗单次采样抖动
    obj_growth = samples[-1][2] - samples[0][2]
    print("\n热身后 RSS 增长: %+.1f MB (预算 %.1f MB)" % (growth, args.budget_mb))
    print("GC 对象数增长: %+d" % obj_growth)
    print("存活线程数: %d (基线 %d)" % (threading.active_count(), base_threads))
    ok = (growth <= args.budget_mb and obj_growth < 5000
          and threading.active_count() == base_threads)
    print("结果:", "PASS - 无持续增长迹象" if ok else "FAIL - 疑似泄漏")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
